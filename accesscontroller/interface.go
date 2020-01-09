package accesscontroller

import (
	"berty.tech/go-ipfs-log/identity"
)

type LogEntry interface {
	GetPayload() []byte
	GetIdentity() *identity.Identity
}

type CanAppendAdditionalContext interface {
	GetLogEntries() []LogEntry
}

type Interface interface {
	CanAppend(LogEntry, identity.Provider, CanAppendAdditionalContext) error
}
