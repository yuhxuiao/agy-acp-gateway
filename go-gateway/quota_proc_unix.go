//go:build unix

package main

import (
	"os/exec"
	"syscall"
	"time"
)

// configureQuotaSubprocess isolates quota/credits/models CLI probes into their own session and
// process group (Setsid: true detaches any controlling /dev/tty per agy#1954 while making the
// child its own process-group leader so context cancellation kills orphaned grandchildren via -pid).
func configureQuotaSubprocess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		pid := cmd.Process.Pid
		if pid > 0 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
		return cmd.Process.Kill()
	}
	cmd.WaitDelay = 500 * time.Millisecond
}
