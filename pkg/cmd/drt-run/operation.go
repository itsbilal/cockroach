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
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type operationImpl struct {
	spec *registry.OperationSpec

	cockroach string // path to main cockroach binary

	deprecatedWorkload string // path to workload binary
	debug              bool   // whether the test is in debug mode.

	// l is the logger that the operation will use for its output.
	l      *logger.Logger
	eventL *eventLogger

	// artifactsDir is the path to the directory holding all the artifacts for
	// this test. It will contain a test.log file and cluster logs.
	artifactsDir string

	mu struct {
		syncutil.RWMutex
		done bool

		// cancel, if set, is called from the t.Fatal() family of functions when the
		// test is being marked as failed (i.e. when the failed field above is also
		// set). This is used to cancel the context passed to t.spec.Run(), so async
		// test goroutines can be notified.
		cancel func()

		// failures added via addFailures, in order
		// A test can have multiple calls to t.Fail()/Error(), with each call
		// referencing 0+ errors. failure captures all the errors
		failures []error

		// numFailures is the number of failures that have been added via addFailures.
		// This can deviate from len(failures) if failures have been suppressed.
		numFailures int

		status   string
		progress float64
	}
	cleanupState map[string]string
}

func (t *operationImpl) Cockroach() string {
	return t.cockroach
}

func (t *operationImpl) DeprecatedWorkload() string {
	return t.deprecatedWorkload
}

func (t *operationImpl) SkipInit() bool {
	// Operations always skip init.
	return true
}

// Spec returns the TestSpec.
func (t *operationImpl) Spec() interface{} {
	return t.spec
}

func (t *operationImpl) Name() string {
	return t.spec.Name
}

// L returns the test's logger.
func (t *operationImpl) L() *logger.Logger {
	return t.l
}

// ReplaceL replaces the test's logger.
func (t *operationImpl) ReplaceL(l *logger.Logger) {
	// TODO(tbg): get rid of this, this is racy & hacky.
	t.l = l
}

func (t *operationImpl) SetCleanupState(key, value string) {
	t.cleanupState[key] = value
}

func (t *operationImpl) GetCleanupState(key string) string {
	return t.cleanupState[key]
}

func (t *operationImpl) status(ctx context.Context, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mu.status = fmt.Sprint(args...)
	if !t.L().Closed() {
		t.L().PrintfCtxDepth(ctx, 3, "operation status: %s", t.mu.status)
	}
}

// Status sets the main status message for the test. When called from the main
// test goroutine (i.e. the goroutine on which TestSpec.Run is invoked), this
// is equivalent to calling WorkerStatus. If no arguments are specified, the
// status message is erased.
func (t *operationImpl) Status(args ...interface{}) {
	t.status(context.TODO(), args...)
}

// IsDebug returns true if the test is in a debug state.
func (t *operationImpl) IsDebug() bool {
	return t.debug
}

// Progress sets the progress (a fraction in the range [0,1]) associated with
// the main test status message. When called from the main test goroutine
// (i.e. the goroutine on which TestSpec.Run is invoked), this is equivalent to
// calling WorkerProgress.
func (t *operationImpl) Progress(frac float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mu.progress = frac
}

var errTestFatal = errors.New("t.Fatal() was called")

// Skip skips the test. The first argument if any is the main message.
// The remaining argument, if any, form the details.
// This implements the skip.SkippableTest interface.
func (t *operationImpl) Skip(args ...interface{}) {
	if len(args) > 0 {
		t.spec.Skip = fmt.Sprint(args[0])
		args = args[1:]
	}
	panic(errTestFatal)
}

// Skipf skips the test. The formatted message becomes the skip reason.
// This implements the skip.SkippableTest interface.
func (t *operationImpl) Skipf(format string, args ...interface{}) {
	t.spec.Skip = fmt.Sprintf(format, args...)
	panic(errTestFatal)
}

// Fatal marks the test as failed, prints the args to t.L(), and calls
// panic(errTestFatal). It can be called multiple times.
//
// If the only argument is an error, it is formatted by "%+v", so it will show
// stack traces and such.
//
// ATTENTION: Since this calls panic(errTestFatal), it should only be called
// from a test's closure. The test runner itself should never call this.
func (t *operationImpl) Fatal(args ...interface{}) {
	t.addFailureAndCancel(1, "", args...)
	//panic(errTestFatal)
}

// Fatalf is like Fatal, but takes a format string.
func (t *operationImpl) Fatalf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
	//panic(errTestFatal)
}

// FailNow implements the TestingT interface.
func (t *operationImpl) FailNow() {
	t.addFailureAndCancel(1, "FailNow called")
	//panic(errTestFatal)
}

// Error implements the TestingT interface
func (t *operationImpl) Error(args ...interface{}) {
	t.addFailureAndCancel(1, "", args...)
}

// Errorf implements the TestingT interface.
func (t *operationImpl) Errorf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
}

func (t *operationImpl) addFailureAndCancel(depth int, format string, args ...interface{}) {
	t.addFailure(depth+1, format, args...)
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

// addFailure depth indicates how many stack frames to skip when reporting the
// site of the failure in logs. `0` will report the caller of addFailure, `1` the
// caller of the caller of addFailure, etc.
func (t *operationImpl) addFailure(depth int, format string, args ...interface{}) {
	if format == "" {
		format = strings.Repeat(" %v", len(args))[1:]
	}
	reportFailure := errors.NewWithDepthf(depth+1, format, args...)

	t.mu.Lock()
	defer t.mu.Unlock()

	msg := reportFailure.Error()

	t.mu.numFailures++
	failureNum := t.mu.numFailures
	failureLog := fmt.Sprintf("failure_%d", failureNum)
	t.L().Printf("operation failure #%d: full stack retained in %s.log: %s", failureNum, failureLog, msg)
	// Also dump the verbose error (incl. all stack traces) to a log file, in case
	// we need it. The stacks are sometimes helpful, but we don't want them in the
	// main log as they are highly verbose.
	{
		cl, err := t.L().ChildLogger(failureLog, logger.QuietStderr, logger.QuietStdout)
		if err == nil {
			// We don't actually log through this logger since it adds an unrelated
			// file:line caller (namely ours). The error already has stack traces
			// so it's better to write only it to the file to avoid confusion.
			if cl.File != nil {
				path := cl.File.Name()
				if len(path) > 0 {
					_ = os.WriteFile(path, []byte(fmt.Sprintf("%s", reportFailure.Error())), 0644)
				}
			}
			cl.Close() // we just wanted the filename
		}
	}
}

func (t *operationImpl) Failed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.failedRLocked()
}

func (t *operationImpl) failedRLocked() bool {
	return t.mu.numFailures > 0
}

func (t *operationImpl) ArtifactsDir() string {
	return t.artifactsDir
}

var _ operation.Operation = &operationImpl{}
