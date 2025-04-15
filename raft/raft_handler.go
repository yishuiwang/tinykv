// raft_handler.go 文件应专注于处理 Raft 协议中的各种消息和请求。

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RequestVote 请求所有其他节点投票
func (r *Raft) RequestVote() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		// 初始化投票记录
		r.votes[id] = false

		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: logTerm,
		}
		r.msgs = append(r.msgs, msg)
	}
	// 如果只有一个节点, 则直接成为leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// HandleRequestVote 处理投票请求
func (r *Raft) HandleRequestVote(m pb.Message) {
	// 1. Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if m.Term > r.Term {
		// Candidate节点不一定会成为Leader，所以只是简单投票给Candidate
		// https://asktug.com/t/topic/273388?replies_to_post_number=3
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if r.Vote == None || r.Vote == m.From {
		// the voter denies its vote if its own log is more up-to-date than that of the candidate.
		if r.moreUp2Date(m.LogTerm, m.Index) {
			log.Warn(r.id, "Reject vote request from", m.From, "because of log")
			r.sendRequestVoteResponse(m.From, true)
			return
		} else {
			r.Vote = m.From
			r.votes[m.From] = true
			r.sendRequestVoteResponse(m.From, false)
			return
		}
	} else {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
}

// HandleVoteResponse 处理投票响应
func (r *Raft) HandleVoteResponse(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = None
		return
	}

	if m.Reject {
		r.votes[m.From] = false
		r.rejectCount++
	} else {
		r.votes[m.From] = true
		r.voteCount++
	}

	// 论文中没有规定收到大多数reject时会转为follower，但是为了通过TestLeaderElectionOverwriteNewerLogs2AB
	// 需要添加rejectCount使candidate转为follower
	// https://asktug.com/t/topic/273439?replies_to_post_number=6
	// https://asktug.com/t/topic/694701/2
	// https://github.com/talent-plan/tinykv/pull/328/files
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.rejectCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// HandleMsgPropose 处理Propose消息
func (r *Raft) HandleMsgPropose(m pb.Message) error {
	log.Infof("raft %d, HandleMsgPropose", r.id)
	if len(m.Entries) == 0 {
		log.Panic("log is empty!")
	}
	// 如果当前节点被移除了集群，丢弃提案
	if _, ok := r.Prs[r.id]; !ok {
		return ErrProposalDropped
	}

	for i, entry := range m.Entries {
		if entry.EntryType == pb.EntryType_EntryConfChange {
			// 存在还未apply的config change
			if r.PendingConfIndex > r.RaftLog.applied {
				log.Errorf("raft %d, already has pending conf change, pendingConfIndex=%d, applied=%d", r.id, r.PendingConfIndex, r.RaftLog.applied)
				return ErrProposalDropped
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
			}
		}
	}

	r.RaftLog.proposeEntries(r.Term, m.Entries)
	// 更新Leader的Next和Match
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

	// 如果只有一个节点, 则直接commit
	if len(r.Prs) == 1 {
		log.Infof("raft %d, only one node, commit", r.id)
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	r.broadcast()

	return nil
}

// handleHeartbeat 处理心跳
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("raft %v handleHeartbeat, m=%+v", r.id, m)
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

// HandleHeartbeatResponse 处理心跳响应
func (r *Raft) HandleHeartbeatResponse(m pb.Message) {
	log.Infof("raft %v handleHeartbeatResponse, m=%+v", r.id, m)
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleAppendEntries 处理AppendEntries
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// msg.index是用来帮助Leader更新follower的pr的
	if r.Term > m.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	// 合法Leader出现，节点必须更新其任期并承认新的 Leader
	r.becomeFollower(m.Term, m.From)

	res := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries)

	r.sendAppendResponse(m.From, !res)
}

// HandleAppendResponse 处理AppendEntries响应
func (r *Raft) HandleAppendResponse(m pb.Message) {
	log.Infof("raft %v handleAppendResponse, m=%+v", r.id, m)
	if m.From == r.leadTransferee {
		r.HandleTransferLeader(m)
	}
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if !m.Reject {
		// 更新pr, m.Index是follower.RaftLog.LastIndex()
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
	} else {
		// 尝试减少Next
		if r.Prs[m.From].Next > 1 {
			r.Prs[m.From].Next--
			log.Errorf("r.prs[%d].Next = %d", m.From, r.Prs[m.From].Next)
			r.sendAppend(m.From)
			return
		}
	}

	r.updateCommit()
}

func (r *Raft) HandleTransferLeader(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if m.From == r.id {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From

	if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if r.id == 2 {
		log.Warnf("raft 2 handleSnapshot, m=%+v", m.Snapshot.Metadata)
	}
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false)
		return
	}

	r.becomeFollower(m.Term, m.From)

	r.RaftLog.ApplySnap(m.Snapshot)

	r.Prs = make(map[uint64]*Progress)
	for _, pr := range m.Snapshot.Metadata.ConfState.Nodes {
		//r.Prs[pr] = &Progress{}
		if pr == r.id {
			r.Prs[pr] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[pr] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	r.sendAppendResponse(m.From, false)
	log.Infof("raft %v r.RaftLog.LastIndex()=%d, r.RaftLog.stabled=%d", r.id, r.RaftLog.LastIndex(), r.RaftLog.stabled)
	log.Infof("raft %v apply snapshot, m=%+v", r.id, m)
}

// 比较谁的日志更新
func (r *Raft) moreUp2Date(term uint64, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	// 如果两个日志的最后条目属于不同的任期，那么拥有较大任期的日志被认为是更新的。
	if term > lastTerm {
		return false
	}
	// 如果两个日志的最后条目属于相同的任期，那么日志更长的那个被认为是更新的。
	if term == lastTerm && index >= r.RaftLog.LastIndex() {
		return false
	}
	return true
}
