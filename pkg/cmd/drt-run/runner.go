// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/logtags"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func ctrlC(ctx context.Context, cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		// Signal runners to stop.
		cancel()
	}()
}

type opsRegistry struct {
	ops []registry.OperationSpec
}

func (o *opsRegistry) MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec {
	return spec.MakeClusterSpec(nodeCount, opts...)
}

func (o *opsRegistry) Add(spec registry.TestSpec) {
	panic("unimplemented")
}

func (o *opsRegistry) AddOperation(spec registry.OperationSpec) {
	o.ops = append(o.ops, spec)
}

func (o *opsRegistry) PromFactory() promauto.Factory {
	panic("unimplemented")
}

var _ registry.Registry = &opsRegistry{}

type opsRunner struct {
	specs       [][]registry.OperationSpec
	config      config
	l           *logger.Logger
	registry    registry.Registry
	parallelism int
	seed        int64
	errChan     chan error
	eventChan   chan Event
}

func (r *opsRunner) runWorker(ctx context.Context, workerIdx int) {
	rng := rand.New(rand.NewSource(r.seed))

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		// TODO(bilal): Pick specs based on cadence, and clean up this code.
		setIdx := rng.Intn(len(r.specs))
		opSpecsSet := r.specs[setIdx]
		opSpec := opSpecsSet[rng.Intn(len(opSpecsSet))]
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opSpec.Timeout)

		operation := &operationImpl{
			spec:               &opSpec,
			cockroach:          r.config.CockroachBinary,
			deprecatedWorkload: r.config.WorkloadBinary,
			debug:              true,
			l:                  r.l,
			artifactsDir:       "artifacts",
			cleanupState:       make(map[string]string),
		}
		operation.mu.cancel = cancel

		c := r.attachCluster()
		c.(*roachprodCluster).o = operation

		operation.Status("starting operation")
		opSpec.Run(ctx, operation, c)

		if opSpec.Cleanup != nil {
			operation.Status(fmt.Sprintf("operation ran successfully; waiting %s before cleanup", opSpec.CleanupWaitTime.String()))
			if opSpec.CleanupWaitTime != 0 {
				select {
				case <-ctx.Done():
					operation.Status("bailing due to cancellation")
					return
				case <-time.After(opSpec.CleanupWaitTime):
				}
			}

			// TODO(bilal): Defer this cleanup to future loop runs.
			opSpec.Cleanup(ctx, operation, c)
		}
	}
}

func (r *opsRunner) attachCluster() cluster.Cluster {
	return makeClusterFromSpec(r.config.ClusterName, r.config.Cluster, r.l, r.config)
}

func (r *opsRunner) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctrlC(ctx, cancel)

	var wg sync.WaitGroup
	wg.Add(r.parallelism)

	for i := 0; i < r.parallelism; i++ {
		i := i
		go func() {
			defer wg.Done()
			r.runWorker(ctx, i)
		}()
	}

	wg.Wait()
}

func makeOpsRunner(parallelism int, config config) (*opsRunner, error) {
	r := &opsRegistry{}
	_, seed := randutil.NewTestRand()

	operations.RegisterOperations(r)
	specs := make([][]registry.OperationSpec, len(config.Operations.Sets))
	for i := range specs {
		filter, err := registry.NewTestFilter([]string{config.Operations.Sets[i].Filter})
		if err != nil {
			return nil, err
		}
		specs[i] = filter.FilterOps(r.ops)
	}
	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		return nil, err
	}

	runner := &opsRunner{
		specs:       specs,
		config:      config,
		registry:    r,
		l:           l,
		seed:        seed,
		parallelism: parallelism,
		errChan:     make(chan error, parallelism),
		eventChan:   make(chan Event, parallelism),
	}

	return runner, nil
}

type workloadRunner struct {
	config config

	workloadsRunning int
	errChan          chan error
	eventChan        chan Event
}

func (w *workloadRunner) runWorker(ctx context.Context, workloadIdx int, workload workloadConfig, l *logger.Logger) {
	ctx = logtags.WithTags(ctx, logtags.SingleTagBuffer("workload", workload.Name))
	for _, step := range workload.Steps {
		args := []string{step.Command, workload.Kind}
		pgUrl, err := roachprod.PgURL(ctx, l, w.config.ClusterName, w.config.CertsDir, roachprod.PGURLOptions{External: true})
		if err != nil {
			fmt.Printf("err: %s\n", err.Error())
			w.errChan <- err
			return
		}
		for i, url := range pgUrl {
			pgUrl[i] = strings.Trim(url, "'")
		}
		args = append(args, pgUrl...)
		args = append(args, step.Args...)
		if strings.HasPrefix(step.Command, "run") {
			// Add prometheus port, starting from 2112.
			args = append(args, fmt.Sprintf("--prometheus-port=%d", 2112+workloadIdx))
		}

		fmt.Printf("running command %s %s\n", w.config.WorkloadBinary, strings.Join(args, " "))
		cmd := exec.CommandContext(ctx, w.config.WorkloadBinary, args...)
		// TODO(bilal): Turn these into events.
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			w.errChan <- err
			return
		}
	}
}

func (w *workloadRunner) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctrlC(ctx, cancel)

	l, err := logger.RootLogger("", logger.NoTee)
	if err != nil {
		return err
	}

	var poolWG sync.WaitGroup
	for i := range w.config.Workloads {
		i := i
		poolWG.Add(1)
		go func() {
			defer poolWG.Done()
			w.workloadsRunning++
			defer func() { w.workloadsRunning-- }()
			w.runWorker(ctx, i, w.config.Workloads[i], l)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-w.errChan:
				l.Errorf("error: %s", err.Error())
			case event := <-w.eventChan:
				l.PrintfCtx(ctx, "event: %s", event.String())
			}
		}
	}()
	poolWG.Wait()
	return nil
}

func makeWorkloadRunner(config config) *workloadRunner {
	parallelism := len(config.Workloads)
	runner := &workloadRunner{
		config:    config,
		errChan:   make(chan error, parallelism),
		eventChan: make(chan Event, parallelism),
	}
	return runner
}
