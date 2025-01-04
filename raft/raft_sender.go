// raft_sender.go 文件应专注于 Raft 协议中与消息发送相关的功能。

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	entry := make([]*pb.Entry, 0)
	for i := pr.Next; i <= r.RaftLog.LastIndex(); i++ {
		entry = append(entry, &r.RaftLog.entries[i])
	}
	preLogTerm := r.RaftLog.entries[pr.Next-1].Term
	preLogIndex := pr.Next - 1

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

func (r *Raft) broadcast() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}
