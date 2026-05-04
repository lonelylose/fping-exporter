package main

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	startDelayMilliseconds = 1000
)

type WorkerSpec struct {
	period  time.Duration
	count   uint
	minWait uint // 保持结构体兼容，但内部逻辑已废弃默认的 10ms，改为自适应计算
}

type Worker struct {
	sync.Mutex
	spec    WorkerSpec
	targets map[TargetSpec]*Target
}

func NewWorker(spec WorkerSpec) *Worker {
	log.Println("New worker (period:", spec.period, ")")
	spec.count = opts.Count

	w := Worker{
		spec:    spec,
		targets: make(map[TargetSpec]*Target),
	}

	go w.cycleRun(startDelayMilliseconds * time.Millisecond)
	return &w
}

func (w *Worker) GetWorkerTarget(ts TargetSpec) *Target {
	w.Lock()
	defer w.Unlock()
	t, ok := w.targets[ts]
	if !ok {
		t = NewTarget(ts)
		w.targets[ts] = t
	}
	return t
}

func (w *Worker) cycleRun(sleepTime time.Duration) {
	time.Sleep(sleepTime)

	// 调度下一个周期的运行
	go w.cycleRun(w.spec.period)

	// 安全提取当前所有目标 IP
	w.Lock()
	hosts := make([]string, 0, len(w.targets))
	for _, t := range w.targets {
		hosts = append(hosts, t.spec.host)
	}
	w.Unlock()

	numHosts := len(hosts)
	if numHosts == 0 {
		return
	}

	// =========================================================================
	// 🚀 Smooth Probing 匀速平滑探测算法
	// =========================================================================
	
	// 1. 预留裕量时间 (防止 fping 执行时间超过 period 导致 exit: -1)
	// 保留 4 秒作为等待高延迟回包和底层进程收尾的缓冲时间 (之前是 2 秒)
	activeSeconds := w.spec.period.Seconds() - 4.0
	if activeSeconds <= 0 {
		activeSeconds = w.spec.period.Seconds() * 0.85 // 极端情况兜底
	}

	// 2. 计算 -p (发送给同一个目标的间隔，单位：毫秒)
	periodMs := int((activeSeconds * 1000.0) / float64(w.spec.count))
	if periodMs < 10 {
		periodMs = 10
	}

	// 3. 计算 -i (发送给不同目标的物理间隔，单位：毫秒)
	// 将 periodMs 均匀分摊给所有目标，实现一条直线的匀速发包
	minWaitMs := periodMs / numHosts
	if minWaitMs < 1 {
		minWaitMs = 1 // fping 的极限是 1ms
	}

	// 兜底保护：防止目标数量过于庞大，导致单轮物理耗时超过了计划的 periodMs
	if minWaitMs*numHosts > periodMs {
		periodMs = minWaitMs * numHosts
	}

	fpingArgs := []string{
		"-q", // quiet
		"-p", strconv.Itoa(periodMs),
		"-C", strconv.FormatUint(uint64(w.spec.count), 10),
		"-i", strconv.Itoa(minWaitMs),
	}

	// 废除原作者 100个并发分组的导致丢包风暴的逻辑
	// 将所有的 IP 全部送入唯一的一个 fping 进程，严格按照 -i 匀速派发
	ctx, cancel := context.WithTimeout(context.Background(), w.spec.period)
	defer cancel()

	cmdString := append(fpingArgs, hosts...)
	cmd := exec.CommandContext(ctx, opts.Fping, cmdString...)
	
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	// 执行并发包
	if err := cmd.Run(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok {
			ws := exitErr.Sys().(syscall.WaitStatus)
			// exit 1: 有目标不通; exit 2: IP 解析失败 (这两项属于正常业务输出)
			if ws.ExitStatus() != 1 && ws.ExitStatus() != 2 {
				log.Printf("fping error (exit: %d). CMD: %v", ws.ExitStatus(), cmdString[:6])
				return
			}
		} else {
			log.Printf("fping execution error: %v", err)
			return
		}
	}
	
	w.addResults(errbuf.String())
}

func (w *Worker) addResults(fpingOutput string) {
	scanner := bufio.NewScanner(strings.NewReader(fpingOutput))
	for scanner.Scan() {
		text := strings.SplitN(scanner.Text(), " : ", 2)
		if len(text) != 2 {
			continue
		}

		host := TargetSpec{host: strings.TrimSpace(text[0])}
		t, ok := w.targets[host]
		if !ok {
			continue
		}

		measurements, err := ParseMeasurements(text[1])
		if err != nil {
			log.Println("Error parsing fping output: ", text[1])
			continue
		}

		t.AddMeasurements(measurements)
	}
}
