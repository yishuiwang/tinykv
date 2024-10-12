// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"log"
	"math/rand/v2"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	baseTimeout int

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := new(Raft)
	r.id = c.ID
	r.RaftLog = newLog(c.Storage)
	r.Term = 0
	r.Vote = None
	r.State = StateFollower
	r.Prs = make(map[uint64]*Progress)
	r.votes = make(map[uint64]bool)
	for _, id := range c.peers {
		r.Prs[id] = &Progress{0, 0}
		r.votes[id] = false
	}
	r.msgs = make([]pb.Message, 0)
	r.Lead = None
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick
	r.baseTimeout = c.ElectionTick
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = None
	r.PendingConfIndex = 0

	for _, v := range c.peers {
		r.Prs[v] = &Progress{0, 1}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: 0, Index: 0, Data: []byte("init")})
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	entry := make([]*pb.Entry, 0)
	for i := pr.Next; i <= r.RaftLog.LastIndex(); i++ {
		entry = append(entry, &r.RaftLog.entries[i])
	}
	// logTerm代表论文中的prevLogTerm
	logTerm := r.RaftLog.entries[pr.Match].Term
	// index代表论文中的prevLogIndex
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Entries: entry,
		LogTerm: logTerm,
		Index:   pr.Match,
	}
	// 更新leader
	r.msgs = append(r.msgs, msg)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

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
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.RequestVote()
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// 超时, 重新选举
			r.becomeCandidate()
			r.RequestVote()
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None

	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0

	r.electionTimeout = r.baseTimeout + rand.IntN(r.baseTimeout)
	// Send RequestVote RPCs to all other servers
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	noop := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	}

	r.RaftLog.entries = append(r.RaftLog.entries, noop)

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}

	// r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{&noop}})
}

// RequestVote 请求所有其他节点投票
func (r *Raft) RequestVote() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		// 初始化投票记录
		r.votes[id] = false
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
	}
	// 如果只有一个节点, 则直接成为leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
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

	// 如果只有一个节点, 则直接commit
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
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
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if r.Vote == None || r.Vote == m.From {
		msg.Reject = false
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
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
	if !m.Reject {
		r.votes[m.From] = true
	}
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}
	if count > len(r.Prs)/2 && r.State == StateCandidate {
		r.becomeLeader()
	}
}

// HandleAppendResponse 处理AppendEntries响应
func (r *Raft) HandleAppendResponse(m pb.Message) {
	if m.Reject {
		// TODO
	}
	// 更新pr, m.Index是follower.RaftLog.LastIndex()
	pr := r.Prs[m.From]
	pr.Match = m.Index
	pr.Next = m.Index + 1
	r.Prs[m.From] = pr

	count := 0
	for _, pr := range r.Prs {
		if pr.Match >= m.Index {
			count++
		}
	}
	if count > len(r.Prs)/2 {
		r.RaftLog.committed = max(r.RaftLog.committed, m.Index)
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.RequestVote()
		case pb.MessageType_MsgRequestVoteResponse:
			r.HandleVoteResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.HandleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.RequestVote()
		case pb.MessageType_MsgRequestVoteResponse:
			r.HandleVoteResponse(m)
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.HandleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			r.HandleMsgPropose(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.HandleVoteResponse(m)
		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.HandleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgAppendResponse:
			r.HandleAppendResponse(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
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
	r.Term = m.Term

	// TODO
	// if len(m.Entries) == 0 {
	// 	return
	// }

	// 添加新的entry
	begin := r.RaftLog.LastIndex() - m.Index + 1
	for i := begin; i < uint64(len(m.Entries)); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, *msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if m.Term < r.Term {
		msg.Reject = true
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		msg.Reject = false
		msg.Term = r.Term
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
