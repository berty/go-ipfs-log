package io // import "berty.tech/go-ipfs-log/io"

import (
	core_iface "github.com/ipfs/interface-go-ipfs-core"
)

// The IpfsServices interface with required IPFS services.
type IpfsServices interface {
	Dag() core_iface.APIDagService
}
