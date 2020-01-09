package accesscontroller

import "berty.tech/go-ipfs-log/identity"

type Default struct{}

// CanAppend Checks whether a given identity can append an entry to the log.
// This implementation allows anyone to write to the log.
func (d *Default) CanAppend(LogEntry, identity.Provider, CanAppendAdditionalContext) error {
	return nil
}

var _ Interface = &Default{}
