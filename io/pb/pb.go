package pb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"

	"berty.tech/go-ipfs-log/errmsg"
	idp "berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/jsonable"
)

type pb struct {
	refClock iface.IPFSLogLamportClock
	refEntry iface.IPFSLogEntry
}

func (p *pb) Write(ctx context.Context, adder ipld.NodeAdder, opts *iface.WriteOpts, obj interface{}) (cid.Cid, error) {
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

	if err := adder.Add(ctx, node); err != nil {
		return cid.Cid{}, err
	}

	return node.Cid(), nil
}

func (p *pb) WriteMany(ctx context.Context, adder ipld.NodeAdder, opts *iface.WriteOpts, objs []interface{}) ([]cid.Cid, error) {
	var err error
	cids := make([]cid.Cid, len(objs))
	nodes := make([]ipld.Node, len(objs))
	for n, obj := range objs {
		var payload []byte

		switch o := obj.(type) {
		case iface.IPFSLogEntry:
			payload, err = json.Marshal(jsonable.ToJsonableEntry(o))
			if err != nil {
				return []cid.Cid{}, err
			}
			break

		case *iface.JSONLog:
			payload, err = json.Marshal(o)
			if err != nil {
				return []cid.Cid{}, err
			}
			break
		default:
			return nil, fmt.Errorf("invalid obj: %v", obj)
		}

		node := &dag.ProtoNode{}
		node.SetData(payload)
		nodes[n] = node
	}

	if err := adder.AddMany(ctx, nodes); err != nil {
		return []cid.Cid{}, err
	}

	return cids, nil
}

func (p *pb) Read(ctx context.Context, getter ipld.NodeGetter, contentIdentifier cid.Cid) (ipld.Node, error) {
	return getter.Get(ctx, contentIdentifier)
}

func (p *pb) ReadMany(ctx context.Context, getter ipld.NodeGetter, contentIdentifiers []cid.Cid) <-chan *ipld.NodeOption {
	return getter.GetMany(ctx, contentIdentifiers)
}

func (p *pb) DecodeRawEntry(node ipld.Node, hash cid.Cid, idProvider idp.Interface) (iface.IPFSLogEntry, error) {
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

func (p *pb) DecodeRawJSONLog(node ipld.Node) (*iface.JSONLog, error) {
	jsonLog := &iface.JSONLog{}
	err := json.Unmarshal(node.RawData(), jsonLog)

	if err != nil {
		return nil, errmsg.ErrCBOROperationFailed.Wrap(err)
	}

	return jsonLog, nil
}

var _io = (*pb)(nil)
var _once sync.Once

func IO(entry iface.IPFSLogEntry, clock iface.IPFSLogLamportClock) (iface.IO, error) {
	_once.Do(func() {
		_io = &pb{
			refClock: clock,
			refEntry: entry,
		}
	})

	return _io, nil
}
