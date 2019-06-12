package accesscontroller // import "berty.tech/go-ipfs-log/accesscontroller"

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
)

type Default struct {
}

func (d *Default) CanAppend(*entry.Entry, *identityprovider.Identity) error {
	return nil
}

var _ Interface = &Default{}
