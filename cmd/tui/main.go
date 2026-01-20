// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cilium/statedb/tui"
)

func main() {
	execCmd := flag.String("exec", "", "Command to run for each db/db/show invocation (required)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if *execCmd == "" {
		log.Fatalf("--exec is required")
	}

	baseCmd := *execCmd
	baseArgs := flag.Args()
	run := func(cmd string) (string, error) {
		args := append(append([]string{}, baseArgs...), strings.Fields(cmd)...)
		c := exec.CommandContext(ctx, baseCmd, args...)
		out, err := c.CombinedOutput()
		return string(out), err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if err := tui.Run(ctx, run, logger); err != nil {
		log.Fatalf("tui: %v", err)
	}
}
