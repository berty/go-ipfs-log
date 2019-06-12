package constants // import "berty.tech/go-ipfs-log/constants"

type WriteFormats uint64

const (
	DAG_PB   WriteFormats = 0
	DAG_CBOR WriteFormats = 1
)

type IPLDLinks uint64

const (
	IPLD_LINK_UNDEFINED IPLDLinks = 0
	IPLD_LINK_NEXT      IPLDLinks = 0
)
