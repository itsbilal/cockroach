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
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
)

// RequestCA makes it possible for a node to request the node-to-node CA certificate.
func (s *adminServer) RequestCA(
	ctx context.Context, req *serverpb.CaRequest,
) (*serverpb.CaResponse, error) {
	cl := security.MakeCertsLocator(s.server.cfg.SSLCertsDir)
	caCert, err := loadCertificateFile(cl.CACertPath())
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to read inter-node cert from disk at %q ",
			cl.CACertPath(),
		)
	}

	res := &serverpb.CaResponse{
		CaCert: caCert,
	}
	return res, nil
}

// RequestCertBundle makes it possible for a node to request its TLS certs from another node.
func (s *adminServer) RequestCertBundle(
	ctx context.Context, req *serverpb.BundleRequest,
) (*serverpb.BundleResponse, error) {
	// TODO(aaron-crl): Validate token sharedSecret is valid.

	certBundle, err := collectLocalCABundle(s.server.cfg.SSLCertsDir)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to collect LocalCABundle",
			err,
		)
	}

	bundleBytes, err := json.Marshal(certBundle)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to marshal LocalCABundle",
			err,
		)
	}

	res := &serverpb.BundleResponse{
		Bundle: bundleBytes,
	}
	return res, nil
}
