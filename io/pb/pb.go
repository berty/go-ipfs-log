package pb

import (
	"context"
	"encoding/json"

	core_iface "github.com/ipfs/boxo/coreiface"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/jsonable"
)

type pb struct {
	refClock iface.IPFSLogLamportClock
	refEntry iface.IPFSLogEntry
}

func (p *pb) Write(ctx context.Context, ipfs core_iface.CoreAPI, obj interface{}, opts *iface.WriteOpts) (cid.Cid, error) {
	var err error
	payload := []byte(nil)

	switch o := obj.(type) {
	case iface.IPFSLogEntry:
		payload, err = json.Marshal(jsonable.ToJsonableEntry(o))
		if err != nil {
			return cid.Undef, err
		}
		break

	case *iface.JSONLog:
		payload, err = json.Marshal(o)
		if err != nil {
			return cid.Undef, err
		}
		break
	}

	node := &dag.ProtoNode{}
	node.SetData(payload)

	if err := ipfs.Dag().Add(ctx, node); err != nil {
		return cid.Cid{}, err
	}

	return node.Cid(), nil
}

func (p *pb) Read(ctx context.Context, ipfs core_iface.CoreAPI, contentIdentifier cid.Cid) (format.Node, error) {
	node, err := ipfs.Dag().Get(ctx, contentIdentifier)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (p *pb) DecodeRawEntry(node format.Node, hash cid.Cid, idProvider idp.Interface) (iface.IPFSLogEntry, error) {
	out := p.refEntry.New()
	entry := &jsonable.EntryV0{}

	pbNode, err := dag.DecodeProtobuf(node.RawData())
	if err != nil {
		return nil, errmsg.ErrPBReadUnmarshalFailed
	}

	if err := json.Unmarshal(pbNode.Data(), entry); err != nil {
		return nil, err
	}

	if err := entry.ToPlain(out, idProvider, p.refClock.New); err != nil {
		return nil, errmsg.ErrEntryDeserializationFailed.Wrap(err)
	}

	out.SetHash(hash)

	return out, nil
}

func (p *pb) DecodeRawJSONLog(node format.Node) (*iface.JSONLog, error) {
	jsonLog := &iface.JSONLog{}
	err := json.Unmarshal(node.RawData(), jsonLog)

	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	return jsonLog, nil
}

var _io = (*pb)(nil)

func IO(entry iface.IPFSLogEntry, clock iface.IPFSLogLamportClock) (iface.IO, error) {
	if _io != nil {
		return _io, nil
	}

	_io := &pb{
		refClock: clock,
		refEntry: entry,
	}

	return _io, nil
}
