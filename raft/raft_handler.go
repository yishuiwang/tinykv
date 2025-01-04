// raft_handler.go 文件应专注于处理 Raft 协议中的各种消息和请求。

package raft

import (
	"log"

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
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	// 1. Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		return
	}
	// the voter denies its vote if its own log is more up-to-date than that of the candidate.
	if m.LogTerm < r.RaftLog.entries[r.RaftLog.LastIndex()].Term {
		// 如果两个日志的最后条目属于不同的任期，那么拥有较大任期的日志被认为是更新的。
		r.msgs = append(r.msgs, msg)
		if m.Term > r.Term {
			// case 1
			r.becomeFollower(m.Term, None)
		}
		return
	}
	if m.LogTerm == r.RaftLog.entries[r.RaftLog.LastIndex()].Term && m.Index < r.RaftLog.LastIndex() {
		// 如果两个日志的最后条目属于相同的任期，那么日志更长的那个被认为是更新的。
		r.msgs = append(r.msgs, msg)
		if m.Term > r.Term {
			// case 2
			r.becomeFollower(m.Term, None)
		}
		return
	}
	// 如果m的任期大于r的任期，则r转为follower
	// 比较谁的日志更新 case 1 和 case 2，决定是否投票给m
	if m.Term > r.Term {
		// Candidate节点不一定会成为Leader，所以只是简单投票给Candidate
		// https://asktug.com/t/topic/273388?replies_to_post_number=3
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		msg.Reject = false
		r.msgs = append(r.msgs, msg)
		return
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if r.Vote == None || r.Vote == m.From {
		msg.Reject = false
	}
	if !msg.Reject {
		r.Vote = m.From
		r.votes[m.From] = true
	}
	r.msgs = append(r.msgs, msg)
}

// HandleVoteResponse 处理投票响应
func (r *Raft) HandleVoteResponse(m pb.Message) {
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
	if r.voteCount > len(r.Prs)/2 && r.State == StateCandidate {
		r.becomeLeader()
	} else if r.rejectCount > len(r.Prs)/2 && r.State == StateCandidate {
		r.becomeFollower(r.Term, None)
	}
}

// HandleMsgPropose 处理Propose消息
func (r *Raft) HandleMsgPropose(m pb.Message) {
	if len(m.Entries) == 0 {
		// TODO:处理空消息
		log.Println("entries is empty")
	}

	for _, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1

		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	// 更新Leader的Next和Match
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

	// 如果只有一个节点, 则直接commit
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	r.broadcast()
}

// handleHeartbeat 处理心跳
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	}
	if m.Term < r.Term {
		msg.Reject = true
		return
	}
	r.becomeFollower(m.Term, m.From)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.msgs = append(r.msgs, msg)
}

// HandleHeartbeatResponse 处理心跳响应
func (r *Raft) HandleHeartbeatResponse(m pb.Message) {
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
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Reject:  false,
	}
	if r.Term > m.Term {
		msg.Reject = true
		msg.Term = r.Term
		r.msgs = append(r.msgs, *msg)
		return
	}
	// 合法Leader出现，节点必须更新其任期并承认新的 Leader
	r.becomeFollower(m.Term, m.From)

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 检查上一条日志是否匹配
	if m.Index > r.RaftLog.LastIndex() {
		msg.Reject = true
		msg.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, *msg)
		return
	}
	if m.LogTerm != r.RaftLog.entries[m.Index].Term {
		msg.Reject = true
		msg.Index = m.Index - 1
		r.msgs = append(r.msgs, *msg)
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 检查冲突
	for i, j := m.Index+1, 0; i <= r.RaftLog.LastIndex() && j < len(m.Entries); i, j = i+1, j+1 {
		if r.RaftLog.entries[i].Term != m.Entries[j].Term {
			r.RaftLog.entries = r.RaftLog.entries[:i]
			// 如果冲突的日志在已提交的日志之前, 则
			r.RaftLog.stabled = min(r.RaftLog.stabled, i-1)
			break
		}
	}
	// 添加新的entry
	begin := r.RaftLog.LastIndex() - m.Index
	for i := begin; i < uint64(len(m.Entries)); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, *msg)

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) > 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}
}

// HandleAppendResponse 处理AppendEntries响应
func (r *Raft) HandleAppendResponse(m pb.Message) {
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
		if r.Prs[m.From].Next > 0 {
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
			return
		}
	}

	r.updateCommit()
}
