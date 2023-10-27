@0xe95db835ec309ec8;
using Go = import "/go.capnp";

$Go.package("foobar");
$Go.import("foobar");

interface RaftTransport {

  using Timestamp = UInt64;
  struct RpcHeader {
    version @0 :Int64;
    id @1 :Data;
    addr @2 :Data;
  }

  enum LogType {
    command @0;
    noop @1;
    addPeerDeprecated @2;
    removePeerDeprecated @3;
    barrier @4;
    configuration @5;
  }

  struct Log {
    index @0 :UInt64;
    term @1 :UInt64;
    type @2 :LogType;
    data @3 :Data;
    extensions @4 :Data;
    appendedAt @5 :Timestamp;
    
  }

  appendStream @0 ();
  appendEntries @1 (
    header :RpcHeader,
    term :UInt64,
    leader :Data,
    prevLogEntry :UInt64,
    prevLogTerm :UInt64,
    entries :List(Log) 
  ) -> (
    header :RpcHeader,
    term :UInt64,
    lastLog :UInt64,
    success :Bool,
    noRetryBackoff :Bool
  );
  requestVote @2 (
    header :RpcHeader,
    term :UInt64,
    candidate :Data,
    lastLogIndex :UInt64,
    lastLogTerm :UInt64,
    leadershipTransfer :Bool
  ) -> (
    header :RpcHeader,
    term :UInt64,
    peers :Data,
    granted :Bool
  );
  timeoutNow @3 (
    header :RpcHeader
  ) -> (
    header :RpcHeader
  );
  installSnapshot @4 (
    header :RpcHeader,
    snapshotVersion :Int64,
    term :UInt64,
    leader :Data,
    lastLogIndex :UInt64,
    lastLogTerm :UInt64,
    peers :Data,
    configuration :Data,
    configurationIndex :UInt64,
    size :Int64,
    data :Data
  ) -> (
    header :RpcHeader,
    term :UInt64,
    success :Bool
  );
}


#service RaftTransport {
#  // AppendEntriesPipeline opens an AppendEntries message stream.
#  rpc AppendEntriesPipeline(stream AppendEntriesRequest) returns (stream AppendEntriesResponse) {}
#
#  // AppendEntries performs a single append entries request / response.
#  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse) {}
#  // RequestVote is the command used by a candidate to ask a Raft peer for a vote in an election.
#  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse) {}
#  // TimeoutNow is used to start a leadership transfer to the target node.
#  rpc TimeoutNow(TimeoutNowRequest) returns (TimeoutNowResponse) {}
#  // InstallSnapshot is the command sent to a Raft peer to bootstrap its log (and state machine) f#rom a snapshot on another peer.
#  rpc InstallSnapshot(stream InstallSnapshotRequest) returns (InstallSnapshotResponse) {}
#}

#message RPCHeader {
#	int64 protocol_version = 1;
#	bytes id = 2;
#	bytes addr = 3;
#}

#message Log {
#	enum LogType {
#		LOG_COMMAND = 0;
#		LOG_NOOP = 1;
#		LOG_ADD_PEER_DEPRECATED = 2;
#		LOG_REMOVE_PEER_DEPRECATED = 3;
#		LOG_BARRIER = 4;
#		LOG_CONFIGURATION = 5;
#	}
#	uint64 index = 1;
#	uint64 term = 2;
#	LogType type = 3;
#	bytes data = 4;
#	bytes extensions = 5;
#	google.protobuf.Timestamp appended_at = 6;
#}

#message AppendEntriesRequest {
#	RPCHeader rpc_header = 1;
#	uint64 term = 2;
#	bytes leader = 3;
#	uint64 prev_log_entry = 4;
#	uint64 prev_log_term = 5;
#	repeated Log entries = 6;
#	uint64 leader_commit_index = 7;
#}

#message AppendEntriesResponse {
#	RPCHeader rpc_header = 1;
#	uint64 term = 2;
#	uint64 last_log = 3;
#	bool success = 4;
#	bool no_retry_backoff = 5;
#}

#message RequestVoteRequest {
#	RPCHeader rpc_header = 1;
#	uint64 term = 2;
#	bytes candidate = 3;
#	uint64 last_log_index = 4;
#	uint64 last_log_term = 5;
#	bool leadership_transfer = 6;
#}


#message RequestVoteResponse {
#	RPCHeader rpc_header = 1;
#	uint64 term = 2;
#	bytes peers = 3;
#	bool granted = 4;
#}


#message TimeoutNowRequest {
#	RPCHeader rpc_header = 1;
#}

#message TimeoutNowResponse {
#	RPCHeader rpc_header = 1;
#}

#// The first InstallSnapshotRequest on the stream contains all the metadata.
#// All further messages contain only data.
#message InstallSnapshotRequest {
#	RPCHeader rpc_header = 1;
#	int64 snapshot_version = 11;
#	uint64 term = 2;
#	bytes leader = 3;
#	uint64 last_log_index = 4;
#	uint64 last_log_term = 5;
#	bytes peers = 6;
#	bytes configuration = 7;
#	uint64 configuration_index = 8;
#	int64 size = 9;
#
#	bytes data = 10;
#}

#message InstallSnapshotResponse {
#	RPCHeader rpc_header = 1;
#	uint64 term = 2;
#	bool success = 3;
#}
