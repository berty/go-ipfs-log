package errmsg // import "berty.tech/go-ipfs-log/errmsg"

// https://dave.cheney.net/2016/04/07/constant-errors
type Error string

func (e Error) Error() string { return string(e) }

const (
	IPFSNotDefined         = Error("ipfs instance not defined")
	IdentityNotDefined     = Error("identity not defined")
	EntriesNotDefined      = Error("entries not defined")
	LogJoinNotDefined      = Error("log to join not defined")
	LogOptionsNotDefined   = Error("log options not defined")
	FetchOptionsNotDefined = Error("fetch options not defined")
)
