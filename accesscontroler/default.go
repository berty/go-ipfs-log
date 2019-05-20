package accesscontroler

import (
	"github.com/berty/go-ipfs-log/entry"
	"github.com/berty/go-ipfs-log/identityprovider"
)

type Default struct {
}

func (d *Default) CanAppend(*entry.Entry, *identityprovider.Identity) error {
	return nil
}

var _ Interface = &Default{}
