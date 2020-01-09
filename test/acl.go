package test

import (
	"errors"

	"berty.tech/go-ipfs-log/accesscontroller"
	"berty.tech/go-ipfs-log/identity"
)

type DenyAll struct {
}

func (*DenyAll) CanAppend(accesscontroller.LogEntry, identity.Provider, accesscontroller.CanAppendAdditionalContext) error {
	return errors.New("denied")
}

type TestACL struct {
	refIdentity *identity.Identity
}

func (t *TestACL) CanAppend(e accesscontroller.LogEntry, p identity.Provider, _ accesscontroller.CanAppendAdditionalContext) error {
	if e.GetIdentity().ID == t.refIdentity.ID {
		return errors.New("denied")
	}

	return nil
}
