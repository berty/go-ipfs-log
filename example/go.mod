module berty.tech/go-ipfs-log/example

go 1.15

require (
	berty.tech/go-ipfs-log v0.0.0
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ipfs v0.9.1
	github.com/ipfs/go-ipfs-config v0.14.0
	github.com/libp2p/go-libp2p v0.14.3
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-peerstore v0.2.7
)

replace berty.tech/go-ipfs-log v0.0.0 => ../

replace github.com/golangci/golangci-lint => github.com/golangci/golangci-lint v1.18.0

replace github.com/go-critic/go-critic v0.0.0-20181204210945-ee9bf5809ead => github.com/go-critic/go-critic v0.3.5-0.20190526074819-1df300866540
