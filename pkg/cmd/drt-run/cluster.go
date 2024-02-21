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
	gosql "database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type roachprodCluster struct {
	name string
	tag  string
	spec spec.ClusterSpec
	o    operation.Operation
	// l is the logger used to log various cluster operations.
	// DEPRECATED for use outside of cluster methods: Use a test's t.L() instead.
	// This is generally set to the current test's logger.
	l *logger.Logger
	// localCertsDir is a local copy of the certs for this cluster. If this is empty,
	// the cluster is running in insecure mode.
	localCertsDir string
	expiration    time.Time
	arch          vm.CPUArch // CPU architecture of the cluster
}

func (c *roachprodCluster) All() option.NodeListOption {
	return c.lister().All()
}

func (c *roachprodCluster) Range(begin, end int) option.NodeListOption {
	return c.lister().Range(begin, end)
}

func (c *roachprodCluster) Nodes(ns ...int) option.NodeListOption {
	return c.lister().Nodes(ns...)
}

func (c *roachprodCluster) lister() option.NodeLister {
	return option.NodeLister{NodeCount: c.spec.NodeCount, Fatalf: c.o.Fatalf}
}

func (c *roachprodCluster) Node(i int) option.NodeListOption {
	return c.lister().Node(i)
}

// status is used to communicate the operation's status. It's a no-op until the
// cluster is passed to an operation, at which point it's hooked up to operation.Status().
func (c *roachprodCluster) status(args ...interface{}) {
	if c.o == nil {
		return
	}
	c.o.Status(args...)
}

func (c *roachprodCluster) Get(ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Get")
	}
	c.status(fmt.Sprintf("getting %v", src))
	defer c.status("")
	return errors.Wrap(roachprod.Get(ctx, l, c.MakeNodes(opts...), src, dest), "cluster.Get")
}

func (c *roachprodCluster) Put(ctx context.Context, src, dest string, nodes ...option.Option) {
	if err := c.PutE(ctx, c.l, src, dest, nodes...); err != nil {
		c.o.Fatal(err)
	}
}

func (c *roachprodCluster) PutE(ctx context.Context, l *logger.Logger, src, dest string, nodes ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}

	c.status("uploading file")
	defer c.status("")
	return errors.Wrap(roachprod.Put(ctx, l, c.MakeNodes(nodes...), src, dest, true /* useTreeDist */), "cluster.PutE")
}

func (c *roachprodCluster) PutLibraries(ctx context.Context, libraryDir string, libraries []string) error {
	panic("unimplemented")
}

func (c *roachprodCluster) Stage(ctx context.Context, l *logger.Logger, application, versionOrSHA, dir string, opts ...option.Option) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) PutString(ctx context.Context, content, dest string, mode os.FileMode, nodes ...option.Option) error {
	temp, err := os.CreateTemp("", filepath.Base(dest))
	if err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	if _, err := temp.WriteString(content); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	temp.Close()
	src := temp.Name()

	if err := os.Chmod(src, mode); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}

	return errors.Wrap(c.PutE(ctx, c.l, src, dest, nodes...), "cluster.PutString")

}

func (c *roachprodCluster) SetRandomSeed(seed int64) {
	// No-op.
}

func (c *roachprodCluster) StartE(ctx context.Context, l *logger.Logger, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option) error {
	return roachprod.Start(ctx, l, c.MakeNodes(opts...), startOpts.RoachprodOpts)
}

func (c *roachprodCluster) Start(ctx context.Context, l *logger.Logger, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option) {
	if err := c.StartE(ctx, l, startOpts, settings, opts...); err != nil {
		c.o.Fatal(err)
	}
}

func (c *roachprodCluster) StopE(ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, nodes ...option.Option) error {
	return errors.Wrap(roachprod.Stop(ctx, l, c.MakeNodes(nodes...), stopOpts.RoachprodOpts), "cluster.StopE")
}

func (c *roachprodCluster) Stop(ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option) {
	if err := c.StopE(ctx, l, stopOpts, opts...); err != nil {
		c.o.Fatal(err)
	}
}

func (c *roachprodCluster) SignalE(ctx context.Context, l *logger.Logger, sig int, nodes ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Signal")
	}
	if c.spec.NodeCount == 0 {
		return nil // unit tests
	}
	return errors.Wrap(roachprod.Signal(ctx, l, c.MakeNodes(nodes...), sig), "cluster.Signal")
}

func (c *roachprodCluster) Signal(ctx context.Context, l *logger.Logger, sig int, nodes ...option.Option) {
	if err := c.SignalE(ctx, l, sig, nodes...); err != nil {
		c.o.Fatal(err)
	}
}

func (c *roachprodCluster) StopCockroachGracefullyOnNode(ctx context.Context, l *logger.Logger, node int) error {
	// A graceful shutdown is sending SIGTERM to the node, then waiting
	// some reasonable amount of time, then sending a non-graceful SIGKILL.
	gracefulOpts := option.DefaultStopOpts()
	gracefulOpts.RoachprodOpts.Sig = 15 // SIGTERM
	gracefulOpts.RoachprodOpts.Wait = true
	gracefulOpts.RoachprodOpts.MaxWait = 60
	if err := c.StopE(ctx, l, gracefulOpts, c.Node(node)); err != nil {
		return err
	}

	// NB: we still call Stop to make sure the process is dead when we
	// try to restart it (in case it takes longer than `MaxWait` for it
	// to finish).
	return c.StopE(ctx, l, option.DefaultStopOpts(), c.Node(node))
}

func (c *roachprodCluster) NewMonitor(ctx context.Context, option ...option.Option) cluster.Monitor {
	panic("unimplemented")
}

func (c *roachprodCluster) StartServiceForVirtualClusterE(ctx context.Context, l *logger.Logger, externalNodes option.NodeListOption, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) StartServiceForVirtualCluster(ctx context.Context, l *logger.Logger, externalNodes option.NodeListOption, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) InternalAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error) {
	return c.addr(ctx, l, node, false)
}

func (c *roachprodCluster) InternalIP(ctx context.Context, l *logger.Logger, nodes option.NodeListOption) ([]string, error) {
	return roachprod.IP(l, c.MakeNodes(nodes), false)
}

func (c *roachprodCluster) ExternalAddr(ctx context.Context, l *logger.Logger, nodes option.NodeListOption) ([]string, error) {
	return c.addr(ctx, l, nodes, true)
}

func (c *roachprodCluster) ExternalIP(ctx context.Context, l *logger.Logger, nodes option.NodeListOption) ([]string, error) {
	var ips []string
	addrs, err := c.ExternalAddr(ctx, l, nodes)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips = append(ips, host)
	}
	return ips, nil
}

func (c *roachprodCluster) SQLPorts(ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string, sqlInstance int) ([]int, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) pgURLErr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions,
) ([]string, error) {
	opts.Secure = c.IsSecure()
	urls, err := roachprod.PgURL(ctx, l, c.MakeNodes(nodes), c.localCertsDir, opts)
	if err != nil {
		return nil, err
	}
	for i, url := range urls {
		urls[i] = strings.Trim(url, "'")
	}
	return urls, nil
}

func (c *roachprodCluster) addr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, external bool,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, l, nodes, roachprod.PGURLOptions{External: external})
	if err != nil {
		return nil, err
	}
	for _, u := range urls {
		parsed, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, parsed.Host)
	}
	return addrs, nil
}

func (c *roachprodCluster) InternalPGUrl(ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions) ([]string, error) {
	return c.pgURLErr(ctx, l, nodes, opts)
}

func (c *roachprodCluster) ExternalPGUrl(ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions) ([]string, error) {
	opts.External = true
	return c.pgURLErr(ctx, l, nodes, opts)
}

func (c *roachprodCluster) Conn(ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption)) *gosql.DB {
	db, err := c.ConnE(ctx, l, node, opts...)
	if err != nil {
		c.o.Fatal(err)
	}
	return db
}

func (c *roachprodCluster) ConnE(ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption)) (db *gosql.DB, retErr error) {
	// NB: errors.Wrap returns nil if err is nil.
	defer func() { retErr = errors.Wrapf(retErr, "connecting to node %d", node) }()

	connOptions := &option.ConnOption{}
	for _, opt := range opts {
		opt(connOptions)
	}
	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node), roachprod.PGURLOptions{
		VirtualClusterName: connOptions.TenantName,
		SQLInstance:        connOptions.SQLInstance,
	})
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(urls[0])
	if err != nil {
		return nil, err
	}

	if connOptions.User != "" {
		u.User = url.User(connOptions.User)
	}

	if connOptions.DBName != "" {
		u.Path = connOptions.DBName
	}
	dataSourceName := u.String()

	if len(connOptions.Options) > 0 {
		vals := make(url.Values)
		for k, v := range connOptions.Options {
			vals.Add(k, v)
		}
		// connect_timeout is a libpq-specific parameter for the maximum wait for
		// connection, in seconds.
		vals.Add("connect_timeout", "60")
		dataSourceName = dataSourceName + "&" + vals.Encode()
	}
	return gosql.Open("postgres", dataSourceName)
}

func (c *roachprodCluster) InternalAdminUIAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) ExternalAdminUIAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) AdminUIPorts(ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string, sqlInstance int) ([]int, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) RunWithDetails(ctx context.Context, testLogger *logger.Logger, options install.RunOptions, args ...string) ([]install.RunResultDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) Run(ctx context.Context, options install.RunOptions, args ...string) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) RunE(ctx context.Context, options install.RunOptions, args ...string) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) RunWithDetailsSingleNode(ctx context.Context, testLogger *logger.Logger, options install.RunOptions, args ...string) (install.RunResultDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) Cloud() string {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) IsLocal() bool {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) IsSecure() bool {
	return c.localCertsDir != ""
}

func (c *roachprodCluster) Architecture() vm.CPUArch {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) WipeE(ctx context.Context, l *logger.Logger, preserveCerts bool, opts ...option.Option) error {
	panic("unimplemented")
}

func (c *roachprodCluster) Wipe(ctx context.Context, preserveCerts bool, opts ...option.Option) {
	panic("unimplemented")
}

func (c *roachprodCluster) DestroyDNS(ctx context.Context, l *logger.Logger) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) Reformat(ctx context.Context, l *logger.Logger, node option.NodeListOption, filesystem string) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) Install(ctx context.Context, l *logger.Logger, nodes option.NodeListOption, software ...string) error {
	//TODO implement me
	panic("implement me")
}

type nodeSelector interface {
	option.Option
	Merge(option.NodeListOption) option.NodeListOption
}

func (c *roachprodCluster) MakeNodes(opts ...option.Option) string {
	var r option.NodeListOption
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			r = s.Merge(r)
		}
	}
	return c.name + r.String()
}

func (c *roachprodCluster) GitClone(ctx context.Context, l *logger.Logger, src, dest, branch string, nodes option.NodeListOption) error {
	cmd := []string{"bash", "-e", "-c", fmt.Sprintf(`'
		if ! test -d %[1]s; then
			git config --global --add safe.directory %[1]s
			git clone -b %[2]s --depth 1 %[3]s %[1]s
  		else
			cd %[1]s
		git fetch origin
		git checkout origin/%[2]s
  		fi
  		'`, dest, branch, src),
	}
	return errors.Wrap(c.RunE(ctx, option.WithNodes(nodes), cmd...), "cluster.GitClone")
}

func (c *roachprodCluster) FetchTimeseriesData(ctx context.Context, l *logger.Logger) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) FetchDebugZip(ctx context.Context, l *logger.Logger, dest string, opts ...option.Option) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) RefetchCertsFromNode(ctx context.Context, node int) error {
	//TODO implement me
	panic("implement me")
}

func (c *roachprodCluster) StartGrafana(ctx context.Context, l *logger.Logger, promCfg *prometheus.Config) error {
	return roachprod.StartGrafana(ctx, l, c.name, c.arch, "", nil, promCfg)
}

func (c *roachprodCluster) StopGrafana(ctx context.Context, l *logger.Logger, dumpDir string) error {
	return roachprod.StopGrafana(ctx, l, c.name, dumpDir)
}

func (c *roachprodCluster) CreateSnapshot(ctx context.Context, snapshotPrefix string) ([]vm.VolumeSnapshot, error) {
	panic("unimplemented")
}

func (c *roachprodCluster) ListSnapshots(ctx context.Context, vslo vm.VolumeSnapshotListOpts) ([]vm.VolumeSnapshot, error) {
	panic("unimplemented")
}

func (c *roachprodCluster) DeleteSnapshots(ctx context.Context, snapshots ...vm.VolumeSnapshot) error {
	panic("unimplemented")
}

func (c *roachprodCluster) ApplySnapshots(ctx context.Context, snapshots []vm.VolumeSnapshot) error {
	panic("unimplemented")
}

func (c *roachprodCluster) GetPreemptedVMs(ctx context.Context, l *logger.Logger) ([]vm.PreemptedVM, error) {
	panic("unimplemented")
}

// Name returns the cluster name, i.e. something like `teamcity-....`
func (c *roachprodCluster) Name() string {
	return c.name
}

// Spec returns the spec underlying the cluster.
func (c *roachprodCluster) Spec() spec.ClusterSpec {
	return c.spec
}

func (c *roachprodCluster) String() string {
	return fmt.Sprintf("%s (%d nodes)", c.name, c.Spec().NodeCount)
}

// makeClusterFromSpec returns a cluster.Cluster from a spec.
func makeClusterFromSpec(name string, spec spec.ClusterSpec, l *logger.Logger, conf config) cluster.Cluster {
	exp := spec.Expiration()
	if name == "local" {
		exp = timeutil.Now().Add(100000 * time.Hour)
	}
	c := &roachprodCluster{
		name:          name,
		spec:          spec,
		l:             l,
		expiration:    exp,
		arch:          spec.Arch,
		localCertsDir: conf.CertsDir,
	}
	return c
}
