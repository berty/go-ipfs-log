// Package accesscontroller defines a default access controller for IPFS Log, it won't actually check anything.
package accesscontroller // import "berty.tech/go-ipfs-log/accesscontroller"

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
)

type Default struct {
}

// CanAppend Checks whether a given identity can append an entry to the log.
// This implementation allows anyone to write to the log.
func (d *Default) CanAppend(*entry.Entry, *identityprovider.Identity) error {
	return nil
}

var _ Interface = &Default{}
