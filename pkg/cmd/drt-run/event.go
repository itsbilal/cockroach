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
	"strings"
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
