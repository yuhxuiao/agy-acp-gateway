//go:build unix

package main

import (
	"os/exec"
	"syscall"
	"time"
)

// configureQuotaSubprocess isolates quota/credits CLI probes into their own process group
// so context cancellation kills orphaned grandchildren holding stdout/stderr pipes open.
func configureQuotaSubprocess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
