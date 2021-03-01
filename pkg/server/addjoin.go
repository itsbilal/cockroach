// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// RequestCA makes it possible for a node to request the node-to-node CA certificate.
func (s *adminServer) RequestCA(
	ctx context.Context, req *serverpb.CaRequest,
) (*serverpb.CaResponse, error) {
	// TODO(aaron-crl): you can pluck data out of ctx from the authentication layer.

	return nil, nil
}

// RequestCertBundle makes it possible for a node to request its TLS certs from another node.
func (s *adminServer) RequestCertBundle(
	ctx context.Context, req *serverpb.BundleRequest,
) (*serverpb.BundleResponse, error) {
	return nil, nil
}
