package accesscontroler

import (
	"github.com/berty/go-ipfs-log/entry"
	"github.com/berty/go-ipfs-log/identityprovider"
)

type Interface interface {
	CanAppend(*entry.Entry, *identityprovider.Identity) error
}