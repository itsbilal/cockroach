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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const cleanupSelectionRatio = 0.5

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

type deferredCleanup struct {
	ctx        context.Context
	op         operation.Operation
	opSpec     *registry.OperationSpec
	eligibleAt time.Time
}

type opsRunner struct {
	specs       [][]registry.OperationSpec
	config      config
	l           *logger.Logger
	registry    registry.Registry
	parallelism int
	seed        int64
	errChan     chan error
	eventLogger *eventLogger

	mu struct {
		syncutil.Mutex

		lastRun          map[string]time.Time
		deferredCleanups []deferredCleanup

		runningOperations []*operationImpl
	}
}

func (r *opsRunner) runningOperations() []*operationImpl {
	ret := make([]*operationImpl, r.parallelism)
	r.mu.Lock()
	defer r.mu.Unlock()
	copy(ret, r.mu.runningOperations)

	return ret
}

func (r *opsRunner) pickCleanup(ctx context.Context, rng *rand.Rand) *deferredCleanup {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.mu.deferredCleanups {
		if r.mu.deferredCleanups[i].eligibleAt.After(time.Now()) {
			continue
		}

		// Do a copy into dc, as we're going to overwrite the entry in
		// deferredCleanups.
		dc := r.mu.deferredCleanups[i]
		copy(r.mu.deferredCleanups[i:], r.mu.deferredCleanups[i+1:])
		r.mu.deferredCleanups = r.mu.deferredCleanups[:len(r.mu.deferredCleanups)-1]
		return &dc
	}
	return nil
}

func (r *opsRunner) pickOperation(ctx context.Context, rng *rand.Rand) *registry.OperationSpec {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		setIdx := rng.Intn(len(r.specs))
		opSpecsSet := r.specs[setIdx]
		opSpec := &opSpecsSet[rng.Intn(len(opSpecsSet))]
		r.mu.Lock()
		lastRun := r.mu.lastRun[opSpec.Name]
		r.mu.Unlock()
		eligibleForNextRun := lastRun.Add(r.config.Operations.Sets[setIdx].Cadence)

		if time.Now().Compare(eligibleForNextRun) >= 0 {
			return opSpec
		}
	}
}

func (r *opsRunner) runWorker(ctx context.Context, workerIdx int) {
	rng := rand.New(rand.NewSource(r.seed + int64(workerIdx)))

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		if rng.Float64() <= cleanupSelectionRatio {
			cleanup := r.pickCleanup(ctx, rng)
			if cleanup != nil {
				// NB: we rely on the ctx stored in the cleanup, as that has a timeout that
				// encapsulates the original run of this operation.
				cleanup.op.Status("running cleanup")
				cleanup.opSpec.Cleanup(cleanup.ctx, cleanup.op, r.attachCluster())
			}
		}

		opSpec := r.pickOperation(ctx, rng)

		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opSpec.Timeout)

		operation := &operationImpl{
			spec:               opSpec,
			cockroach:          r.config.CockroachBinary,
			deprecatedWorkload: r.config.WorkloadBinary,
			debug:              true,
			l:                  r.l,
			eventL:             r.eventLogger,
			artifactsDir:       "artifacts",
			cleanupState:       make(map[string]string),
		}
		operation.mu.cancel = cancel

		c := r.attachCluster()
		c.(*roachprodCluster).o = operation

		operation.Status("starting operation")
		r.mu.Lock()
		r.mu.lastRun[opSpec.Name] = time.Now()
		r.mu.runningOperations[workerIdx] = operation
		r.mu.Unlock()

		opSpec.Run(ctx, operation, c)

		r.mu.Lock()
		// TODO(bilal): surface
		r.mu.runningOperations[workerIdx] = nil
		r.mu.Unlock()

		if opSpec.Cleanup != nil {
			operation.Status(fmt.Sprintf("operation ran successfully; waiting %s before scheduling cleanup", opSpec.CleanupWaitTime.String()))

			r.mu.Lock()
			r.mu.deferredCleanups = append(r.mu.deferredCleanups, deferredCleanup{
				ctx:        ctx,
				op:         operation,
				opSpec:     opSpec,
				eligibleAt: time.Now().Add(opSpec.CleanupWaitTime),
			})
			r.mu.Unlock()
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

func makeOpsRunner(parallelism int, config config, eventLogger *eventLogger) (*opsRunner, error) {
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
		eventLogger: eventLogger,
		seed:        seed,
		parallelism: parallelism,
		errChan:     make(chan error, parallelism),
	}
	runner.mu.lastRun = make(map[string]time.Time)
	runner.mu.runningOperations = make([]*operationImpl, parallelism)

	return runner, nil
}

type workloadRunner struct {
	config           config
	eventL           *eventLogger
	workloadsRunning int
	errChan          chan error
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
			}
		}
	}()
	poolWG.Wait()
	return nil
}

func makeWorkloadRunner(config config, eventL *eventLogger) *workloadRunner {
	parallelism := len(config.Workloads)
	runner := &workloadRunner{
		config:  config,
		eventL:  eventL,
		errChan: make(chan error, parallelism),
	}
	return runner
}
