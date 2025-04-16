// raft_sender.go 文件应专注于 Raft 协议中与消息发送相关的功能。

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	preLogIndex := pr.Next - 1
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	// 1.发送的日志已经被压缩,改为发送快照
	// 2.节点刚被创建，发送快照
	if errors.Is(err, ErrCompacted) || pr.Next <= util.RaftInvalidIndex {
		// log.Warnf("entry compacted, send snapshot to %d", to)
		return r.sendSnapshot(to)
	}
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex < pr.Next {
		// log.Warn("no new entries to send", "lastIndex", lastIndex, "pr.Next", pr.Next)
		// return false
	}

	entries, _ := r.RaftLog.Entries(pr.Next, lastIndex+1)
	entry := make([]*pb.Entry, 0)
	for _, e := range entries {
		entry = append(entry, &e)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Entries: entry,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
	}
	r.msgs = append(r.msgs, msg)
	log.Infof("raft %d send append to %d, len entries %v", r.id, to, len(entries))
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) broadcast() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
		// log.Warn("snapshot temporarily unavailable")
		return false
	}
	log.Infof("snapshot index %d, term %d, to %d", snapshot.Metadata.Index, snapshot.Metadata.Term, to)
	log.Infof("raft commit index %d, lastIndex %d", r.RaftLog.committed, r.RaftLog.LastIndex())

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}
