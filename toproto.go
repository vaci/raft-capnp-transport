package foobar

import (
	"github.com/hashicorp/raft"
)

func encodeLogType(s raft.LogType) RaftTransport_LogType {
	switch s {
	case raft.LogCommand:
		return RaftTransport_LogType_command
	case raft.LogNoop:
		return RaftTransport_LogType_noop
	case raft.LogAddPeerDeprecated:
		return RaftTransport_LogType_addPeerDeprecated
	case raft.LogRemovePeerDeprecated:
		return RaftTransport_LogType_removePeerDeprecated
	case raft.LogBarrier:
		return RaftTransport_LogType_barrier
	case raft.LogConfiguration:
		return RaftTransport_LogType_configuration
	default:
		panic("invalid LogType")
	}
}
