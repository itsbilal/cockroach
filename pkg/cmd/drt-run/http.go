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
	"net"
	"net/http"
)

type httpHandler struct {
	ctx context.Context
	w   *workloadRunner
	o   *opsRunner
}

func (h *httpHandler) startHTTPServer(httpPort int, bindTo string) error {
	http.HandleFunc("/", h.serve)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindTo, httpPort))
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(listener, nil /* handler */); err != nil {
			panic(err)
		}
	}()
	bindToDesc := "all network interfaces"
	if bindTo != "" {
		bindToDesc = bindTo
	}
	fmt.Printf("HTTP server listening on port %d on %s: http://%s:%d/\n", httpPort, bindToDesc, bindTo, httpPort)
	return nil
}

func (h *httpHandler) writeRunningOperations(rw http.ResponseWriter) {
	runningOps := h.o.runningOperations()
	fmt.Fprintf(rw, "<table style=\"border: 1px solid black;\">\n<tr><th>Worker</th><th>Status</th><th>Progress</th></tr>\n")
	for i := range runningOps {
		fmt.Fprintf(rw, "<tr>\n")
		fmt.Fprintf(rw, "<td>%d</td>", i)
		if runningOps[i] == nil {
			fmt.Fprintf(rw, "<td>idle</td><td></td></tr>")
			continue
		}
		func() {
			runningOps[i].mu.Lock()
			defer runningOps[i].mu.Unlock()

			fmt.Fprintf(rw, "<td>%s</td><td>%d</td></tr>", runningOps[i].mu.status, int(100*runningOps[i].mu.progress))
		}()
	}
	fmt.Fprintf(rw, "</table>")
}

func (h *httpHandler) serve(rw http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(rw, "<!DOCTYPE html>\n")
	fmt.Fprintf(rw, "<html lang=\"en\">\n")
	fmt.Fprintf(rw, "<head><title>DRT Run</title><meta charset=\"utf-8\"></head>\n")
	fmt.Fprintf(rw, "<body>\n")

	fmt.Fprintf(rw, "<h3>Workloads</h3><hr>\n")

	fmt.Fprintf(rw, "<p>workloads running: %d</p>", h.w.workloadsRunning)

	fmt.Fprintf(rw, "<p>Workloads:</p>\n<ul>")
	for _, w := range h.w.config.Workloads {
		fmt.Fprintf(rw, "<li>%s (kind %s)</li>", w.Name, w.Kind)
	}
	fmt.Fprintf(rw, "</ul>\n")

	fmt.Fprintf(rw, "<h3>Operations</h3><hr>\n")
	h.writeRunningOperations(rw)

	fmt.Fprintf(rw, "<h4>Operation events</h4>\n")
	fmt.Fprintf(rw, "<p>TODO</p>\n")

	fmt.Fprintf(rw, "</body>\n</html>")

}
