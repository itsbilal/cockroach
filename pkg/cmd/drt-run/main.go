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
	"os"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var rootCmd = &cobra.Command{
	Use:   "drt-run [config-file] (flags)",
	Short: "tool for managing long-running workloads and roachtest operations",
	Long: `drt-run is a tool for managing long-running workloads and a regular
cadence of workload operations

Examples:

  drt-run config.yaml --operations "add-.*"
  drt-run config.yaml
  drt-run config.yaml --cluster "test-cluster"

The above commands will create a "local" 3 node cluster, start a cockroach
cluster on these nodes, run a sql command on the 2nd node, stop, wipe and
destroy the cluster.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := args[0]
		return runDrt(configFile)
	},
}

var (
	clusterName string
)

func runDrt(configFile string) (retErr error) {
	errChan := make(chan error, 1)
	defer func() {
		if retErr == nil {
			select {
			case err := <-errChan:
				retErr = err
				return
			default:
				return
			}
		}
	}()
	c := config{}
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f, err := os.Open(configFile)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&c); err != nil {
		return err
	}
	wr := makeWorkloadRunner(c)
	wg.Add(1)

	go func() {
		defer close(errChan)
		defer wg.Done()
		if err := wr.Run(ctx); err != nil {
			errChan <- err
		}
	}()

	or, err := makeOpsRunner(c.Operations.Parallelism /* parallelism */, c)
	if err != nil {
		return err
	}
	or.Run(ctx)

	return nil
}

func main() {
	_ = roachprod.InitProviders()
	rootCmd.Flags().StringVar(&clusterName, "cluster", "", "Specifies"+
		"the roachprod cluster name to use for this test")
	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
