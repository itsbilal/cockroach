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
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type EventType int

const (
	EventOutput EventType = iota
	EventStart
	EventFinish
	EventError
)

type EventSource bool

const (
	SourceOperation EventSource = false
	SourceWorkload  EventSource = true
)

const perWorkloadEventRetention = 1000
const operationEventRetention = 1000

type Event struct {
	Type       EventType
	Source     EventSource
	SourceName string
	Details    string
}

// String() implements the Stringer interface.
func (e *Event) String() string {
	var builder strings.Builder
	switch e.Source {
	case SourceWorkload:
		fmt.Fprintf(&builder, "workload %s", e.SourceName)
	case SourceOperation:
		fmt.Fprintf(&builder, "operation %s", e.SourceName)
	}
	switch e.Type {
	case EventOutput:
		fmt.Fprintf(&builder, ": %s", e.Details)
	case EventFinish:
		fmt.Fprintf(&builder, " finished with %s", e.Details)
	case EventStart:
		builder.WriteString(" started")
		if e.Details != "" {
			fmt.Fprintf(&builder, " with %s", e.Details)
		}
	case EventError:
		fmt.Fprintf(&builder, " returned an error %s", e.Details)
	}
	return builder.String()
}

type workloadEvents struct {
	workloadName string
	events       []Event
}

type eventLogger struct {
	outputFile io.Writer

	mu struct {
		syncutil.Mutex
		workloadEvents  []workloadEvents
		operationEvents []Event
	}
}

func (l *eventLogger) logOperationEvent(ev Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO(bilal): make this a circular buffer.
	l.mu.operationEvents = append(l.mu.operationEvents, ev)
	if len(l.mu.operationEvents) > operationEventRetention {
		copy(l.mu.operationEvents, l.mu.operationEvents[1:])
		l.mu.operationEvents = l.mu.operationEvents[:len(l.mu.operationEvents)-1]
	}
}

func makeEventLogger(out io.Writer, c config) *eventLogger {
	el := &eventLogger{
		outputFile: out,
	}
	el.mu.workloadEvents = make([]workloadEvents, len(c.Workloads))
	for i := range c.Workloads {
		el.mu.workloadEvents[i].workloadName = c.Workloads[i].Name
	}
	el.mu.operationEvents = make([]Event, 0, operationEventRetention)
	return el
}
