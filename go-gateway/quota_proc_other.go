//go:build !unix

package main

import (
	"os/exec"
	"time"
)

func configureQuotaSubprocess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.WaitDelay = 500 * time.Millisecond
}
