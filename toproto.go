package transport

import (
	fb "foobar"
	"github.com/hashicorp/raft"
)

func encodeLogType(s raft.LogType) pb.Log_LogType {
	switch s {
	case raft.LogCommand:
		return tr.Log_LOG_COMMAND
	case raft.LogNoop:
		return tr.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return tr.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return tr.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return tr.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return tr.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}
