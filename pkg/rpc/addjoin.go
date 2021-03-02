// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"math"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

/// TODO(aaron-crl): This needs to be in an exported function in package pkg/rpc because it needs access to non-exported snappyCompressor.

// GetAddJoinDialOptions returns a standard list of DialOptions for use during
// Add/Join operations.
func GetAddJoinDialOptions() []grpc.DialOption {
	// Populate the dialOpts.
	var dialOpts []grpc.DialOption

	// Populate a valid tlsConfig here
	// dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))

	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
		grpc.MaxCallSendMsgSize(math.MaxInt32),
	))
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor((snappyCompressor{}).Name())))
	dialOpts = append(dialOpts, grpc.WithNoProxy())
	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = maxBackoff
	dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffConfig}))
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(clientKeepalive))
	dialOpts = append(dialOpts,
		grpc.WithInitialWindowSize(initialWindowSize),
		grpc.WithInitialConnWindowSize(initialConnWindowSize))

	return dialOpts
}
