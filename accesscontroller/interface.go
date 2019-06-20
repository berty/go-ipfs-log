package accesscontroller // import "berty.tech/go-ipfs-log/accesscontroller"

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
)

type Interface interface {
	CanAppend(*entry.Entry, identityprovider.Interface) error
}
