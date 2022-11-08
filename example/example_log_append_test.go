package example

import (
	"context"
	"fmt"
	"io/ioutil"

	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	config "github.com/ipfs/go-ipfs-config"
	ipfs_core "github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	ipfs_libp2p "github.com/ipfs/kubo/core/node/libp2p"
	ipfs_repo "github.com/ipfs/kubo/repo"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	log "berty.tech/go-ipfs-log"
)

func buildHostOverrideExample(ctx context.Context, id peer.ID, ps pstore.Peerstore, options ...libp2p.Option) (host.Host, error) {
	return ipfs_libp2p.DefaultHostOption(ctx, id, ps, options...)
}

func newRepo() (ipfs_repo.Repo, error) {
	// Generating config
	cfg, err := config.Init(ioutil.Discard, 2048)
	if err != nil {
		return nil, err
	}

	// Listen on local interface only
	cfg.Addresses.Swarm = []string{
		"/ip4/127.0.0.1/tcp/0",
	}

	// Do not bootstrap on ipfs node
	cfg.Bootstrap = []string{}

	return &ipfs_repo.Mock{
		D: dssync.MutexWrap(datastore.NewMapDatastore()),
		C: *cfg,
	}, nil
}

func buildNode(ctx context.Context) (*ipfs_core.IpfsNode, error) {
	r, err := newRepo()
	if err != nil {
		return nil, err
	}

	cfg := &ipfs_core.BuildCfg{
		Online: true,
		Repo:   r,
		Host:   buildHostOverrideExample,
	}

	return ipfs_core.NewNode(ctx, cfg)
}

func Example_logAppend() {
	ctx := context.Background()

	// Build Ipfs Node A
	nodeA, err := buildNode(ctx)
	if err != nil {
		panic(err)
	}

	// Build Ipfs Node B
	nodeB, err := buildNode(ctx)
	if err != nil {
		panic(err)
	}

	nodeBInfo := pstore.PeerInfo{
		ID:    nodeB.Identity,
		Addrs: nodeB.PeerHost.Addrs(),
	}

	// Connecting NodeA with NodeB
	if err := nodeA.PeerHost.Connect(ctx, nodeBInfo); err != nil {
		panic(fmt.Errorf("connect error: %s", err))
	}

	serviceA, err := coreapi.NewCoreAPI(nodeA)
	if err != nil {
		panic(fmt.Errorf("coreapi error: %s", err))
	}

	serviceB, err := coreapi.NewCoreAPI(nodeB)
	if err != nil {
		panic(fmt.Errorf("coreapi error: %s", err))
	}

	// Fill up datastore with identities
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ks, err := keystore.NewKeystore(ds)
	if err != nil {
		panic(err)
	}

	// Create identity A
	identityA, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userA",
		Type:     "orbitdb",
	})

	if err != nil {
		panic(err)
	}

	// Create identity B
	identityB, err := idp.CreateIdentity(&idp.CreateIdentityOptions{
		Keystore: ks,
		ID:       "userB",
		Type:     "orbitdb",
	})

	if err != nil {
		panic(err)
	}

	// creating log
	logA, err := log.NewLog(serviceA, identityA, &log.LogOptions{ID: "A"})
	if err != nil {
		panic(err)
	}

	// nodeA Append data (hello world)"
	_, err = logA.Append(ctx, []byte("hello world"), nil)
	if err != nil {
		panic(fmt.Errorf("append error: %s", err))
	}

	h, err := logA.ToMultihash(ctx)
	if err != nil {
		panic(fmt.Errorf("ToMultihash error: %s", err))
	}

	res, err := log.NewFromMultihash(ctx, serviceB, identityB, h, &log.LogOptions{}, &log.FetchOptions{})
	if err != nil {
		panic(fmt.Errorf("NewFromMultihash error: %s", err))
	}

	// nodeB lookup logA
	fmt.Println(res.ToString(nil))

	// Output: hello world
}
