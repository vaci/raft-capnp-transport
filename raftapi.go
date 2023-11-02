package foobar

import (
	"context"
	"io"
	"sync"
	"github.com/hashicorp/raft"
	bs "github.com/vaci/raft-capnp-transport/bs"
)

// These are calls from the Raft engine that we need to send out over gRPC.
type RaftTransportServer struct{}

func (RaftTransportServer) RequestVote(ctx context.Context, call RaftTransport_requestVote) error {
  return nil
}

type raftAPI struct {
	manager *Manager
}

type conn struct {
	//clientConn *grpc.ClientConn
	client     RaftTransport
	mtx        sync.Mutex
}

func decodeRPCHeader(m *RaftTransport_RpcHeader) raft.RPCHeader {
	
	reply := raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(m.Version()),
	}
	{
		value, err := m.Id()
		if err != nil {
			reply.ID = value
		}
	}
	{
		value, err := m.Addr()
		if err != nil {
			reply.Addr = value
		}
	}
	
	return reply
}

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (r raftAPI) Consumer() <-chan raft.RPC {
	return r.manager.rpcChan
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (r raftAPI) LocalAddr() raft.ServerAddress {
	return r.manager.localAddress
}

func (r raftAPI) getPeer(id raft.ServerID, target raft.ServerAddress) (RaftTransport, error) {
	r.manager.connectionsMtx.Lock()
	c, ok := r.manager.connections[id]
	if !ok {
		c = &conn{}
		c.mtx.Lock()
		r.manager.connections[id] = c
	}
	r.manager.connectionsMtx.Unlock()
	if ok {
		c.mtx.Lock()
	}
	defer c.mtx.Unlock()
	//if c.clientConn == nil {
		//conn, err := rpc.NewConn(rpc.NewStreamTransport(rwc), nil)
	//	if err != nil {
	//		return nil, err
	//	}
	//	c.clientConn = conn
	//	c.client = pb.NewRaftTransportClient(conn)
	//}
	return c.client, nil
}

func (r raftAPI) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	c, err := r.getPeer(id, target)
	if err != nil {
		return err
	}
	method, releaseMethod := c.AppendEntries(context.TODO(), func(params RaftTransport_appendEntries_Params) error {
		params.SetTerm(args.Term)
		return nil
	})
		defer releaseMethod()
	reply, err := method.Struct()
	if err != nil {
		return err
	}
	header, err := reply.Header()
	if err != nil {
		return err
	}
	*resp = raft.AppendEntriesResponse{
		RPCHeader: decodeRPCHeader(&header),
		Term: reply.Term(),
	}
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (r raftAPI) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	c, err := r.getPeer(id, target)
	if err != nil {
		return err
	}
	method, releaseMethod := c.RequestVote(context.TODO(), func(params  RaftTransport_requestVote_Params) error {
		params.SetTerm(args.Term)
		params.SetCandidate(args.Candidate)
		params.SetLastLogIndex(args.LastLogIndex)
		params.SetLastLogTerm(args.LastLogTerm)
		params.SetLeadershipTransfer(args.LeadershipTransfer)
		return nil
	})
	defer releaseMethod()
	reply, err := method.Struct()
	if err != nil {
		return err
	}
	header, err := reply.Header()
	if err != nil {
		return err
	}
	*resp = raft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(&header),
		Term: reply.Term(),
		//Peers: reply.Peers(),
		Granted: reply.Granted(),
	}

	return nil
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (r raftAPI) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	c, err := r.getPeer(id, target)
	if err != nil {
		return err
	}
	method, releaseMethod := c.TimeoutNow(context.TODO(), func(params  RaftTransport_timeoutNow_Params) error {
		return nil
	})
	defer releaseMethod()
	reply, err := method.Struct()
	if err != nil {
		return err
	}
	header, err := reply.Header()
	if err != nil {
		return err
	}
	*resp = raft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(&header),
	}
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (r raftAPI) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	c, err := r.getPeer(id, target)
	if err != nil {
		return err
	}

	method, releaseMethod := c.InstallSnapshot(context.TODO(), func(params  RaftTransport_installSnapshot_Params) error {
		params.SetSnapshotVersion(int64(args.SnapshotVersion))
		params.SetTerm(args.Term)
		params.SetLeader(args.Leader)
		params.SetLastLogIndex(args.LastLogIndex)
		params.SetLastLogTerm(args.LastLogTerm)
		params.SetPeers(args.Peers)
		params.SetConfiguration(args.Configuration)
		params.SetConfigurationIndex(args.ConfigurationIndex)
		return nil
	})
	defer releaseMethod()
	stream := method.Snapshot()

	var buf [16384]byte
	for {
		n, err := data.Read(buf[:])
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return err
		}
		stream.Write(context.TODO(), func(params bs.ByteStream_write_Params) error {
			params.SetBytes(buf[:n])
			return nil
		})
		if err != nil {
			return err
		}
	}
	future, releaseMethod := stream.End(context.TODO(), nil)
	defer releaseMethod()
	_, err = future.Struct()
	if err != nil {
		return err
	}

	if err := c.WaitStreaming(); err != nil {
		return err
	}
	
	reply, err := method.Struct()
	if err != nil {
		return err
	}

	header, err := reply.Header()
	*resp = raft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(&header),
		Term: reply.Term(),
		Success: reply.Success(),
	}
	return nil
}

// EncodePeer is used to serialize a peer's address.
func (r raftAPI) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (r raftAPI) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (r raftAPI) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.manager.heartbeatFuncMtx.Lock()
	r.manager.heartbeatFunc = cb
	r.manager.heartbeatFuncMtx.Unlock()
}
