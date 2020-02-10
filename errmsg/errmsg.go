// Package errmsg defines error messages used by the Go version of IPFS Log.
package errmsg // import "berty.tech/go-ipfs-log/errmsg"
import (
	"fmt"
)

// https://dave.cheney.net/2016/04/07/constant-errors

type Error string

func (e Error) Error() string { return string(e) }

func (e Error) Wrap(inner error) error { return fmt.Errorf("%s: %w", e, inner) }

const (
	IPFSNotDefined               = Error("ipfs instance not defined")
	IdentityNotDefined           = Error("identity not defined")
	EntriesNotDefined            = Error("entries not defined")
	LogJoinNotDefined            = Error("log to join not defined")
	LogOptionsNotDefined         = Error("log options not defined")
	PayloadNotDefined            = Error("payload not defined")
	FetchOptionsNotDefined       = Error("fetch options not defined")
	EmptyLogSerialization        = Error("can't serialize an empty log")
	LogIDNotDefined              = Error("log ID not defined")
	EntryNotDefined              = Error("entry is not defined")
	KeyNotDefined                = Error("key is not defined")
	SigNotDefined                = Error("signature is not defined")
	SigNotVerified               = Error("signature could not verified")
	FilterLTNotFound             = Error("entry specified at LT not found")
	FilterLTENotFound            = Error("entry specified at LTE not found")
	TiebreakerBogus              = Error("log's tiebreaker function has returned zero and therefore cannot be")
	IdentityProviderNotDefined   = Error("an identity provider constructor needs to be given as an option")
	IdentityProviderNotSupported = Error("identity provider is not supported")
	IteratorOptionsNotDefined    = Error("no iterator options specified")
	OutputChannelNotDefined      = Error("no output channel specified")
	NotSecp256k1PubKey           = Error("supplied key is not a valid Secp256k1 public key")
	KeystoreNotDefined           = Error("keystore not defined")
	KeyNotInKeystore             = Error("private signing key not found from Keystore")
	InvalidPubKeyFormat          = Error("unable to unmarshal public key")
	InvalidPrivKeyFormat         = Error("unable to unmarshal private key")
	LogAppendFailed              = Error("log append failed")
	LogAppendDenied              = Error("log append denied")
	LogTraverseFailed            = Error("log traverse failed")
	LogJoinFailed                = Error("log join failed")
	LogFromMultiHash             = Error("new from entry hash failed")
	LogFromEntryHash             = Error("new from multi hash failed")
	LogFromJSON                  = Error("new from JSON failed")
	LogFromEntry                 = Error("new from entry failed")
	EntryNotHashable             = Error("entry is hashable")
)
