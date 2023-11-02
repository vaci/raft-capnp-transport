// Package transport provides a Transport for github.com/hashicorp/raft over Capnproto.
package foobar

import (
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

var (
	errCloseErr = errors.New("error closing connections")
)

type Manager struct {
	localAddress raft.ServerAddress
	//dialOptions  []grpc.DialOption

	rpcChan          chan raft.RPC
	heartbeatFunc    func(raft.RPC)
	heartbeatFuncMtx sync.Mutex
	heartbeatTimeout time.Duration

	connectionsMtx sync.Mutex
	connections    map[raft.ServerID]*conn
}

// New creates both components of raft-grpc-transport: a gRPC service and a Raft Transport.
func New(localAddress raft.ServerAddress, options ...Option) *Manager {
	m := &Manager{
		localAddress: localAddress,
		//dialOptions:  dialOptions,

		rpcChan:     make(chan raft.RPC),
		connections: map[raft.ServerID]*conn{},
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// Transport returns a raft.Transport that communicates over gRPC.
func (m *Manager) Transport() raft.Transport {
	return raftAPI{m}
}

func (m *Manager) Close() error {
	m.connectionsMtx.Lock()
	defer m.connectionsMtx.Unlock()

	err := errCloseErr
	for _, conn := range m.connections {
		// Lock conn.mtx to ensure Dial() is complete
		conn.mtx.Lock()
		conn.mtx.Unlock()
		closeErr := conn.clientConn.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}

	if err != errCloseErr {
		return err
	}

	return nil
}
