module berty.tech/go-ipfs-log/example

go 1.15

require (
	berty.tech/go-ipfs-log v0.0.0
	github.com/gopherjs/gopherjs v0.0.0-20190812055157-5d271430af9f // indirect
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ipfs v0.13.1
	github.com/ipfs/go-ipfs-config v0.18.0
	github.com/libp2p/go-libp2p v0.19.4
	github.com/libp2p/go-libp2p-core v0.15.1
	github.com/libp2p/go-libp2p-peerstore v0.6.0
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
)

replace berty.tech/go-ipfs-log v0.0.0 => ../

replace github.com/golangci/golangci-lint => github.com/golangci/golangci-lint v1.18.0

replace github.com/go-critic/go-critic v0.0.0-20181204210945-ee9bf5809ead => github.com/go-critic/go-critic v0.3.5-0.20190526074819-1df300866540
