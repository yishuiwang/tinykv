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
	"math/rand/v2"

	"github.com/pingcap-incubator/tinykv/log"
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

	voteCount   int
	rejectCount int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := new(Raft)
	r.id = c.ID
	r.RaftLog = newLog(c.Storage)
	// 恢复hardState
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	if hardState.Commit > 0 {
		r.RaftLog.committed = hardState.Commit
	}
	r.State = StateFollower
	r.Prs = make(map[uint64]*Progress)
	r.votes = make(map[uint64]bool)
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{lastIndex, lastIndex + 1}
		} else {
			r.Prs[id] = &Progress{0, lastIndex + 1}
		}
	}
	r.msgs = make([]pb.Message, 0)
	r.Lead = None
	r.heartbeatTimeout = c.HeartbeatTick
	r.baseTimeout = c.ElectionTick
	// 防止多个peer同时竞选
	r.electionTimeout = r.baseTimeout + rand.IntN(r.baseTimeout)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = None
	r.PendingConfIndex = 0

	log.Info("newRaft", "id", r.id, "term", r.Term, "vote", r.Vote, "state", r.State, "peers", r.Prs)
	return r
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
	r.voteCount = 0
	r.rejectCount = 0
	r.leadTransferee = None
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.voteCount = 1
	r.rejectCount = 0

	r.electionTimeout = r.baseTimeout + rand.IntN(r.baseTimeout)
	// Send RequestVote RPCs to all other servers

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// 初始化Prs
	for id := range r.Prs {
		r.Prs[id].Match = 0
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	}
	// 更新Leader的Next和Match
	log.Errorf("raft %d becomeLeader,lastIndex, %d,entries: %v", r.id, r.RaftLog.LastIndex(), r.RaftLog.allEntries())
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// Leader should propose a noop entry on its term
	noop := pb.Entry{
		Term: r.Term,
		//Index: r.RaftLog.LastIndex() + 1,
		Data: nil,
	}
	//
	//log.Infof("raft %v becomeLeader, noop %v", r.id, noop)
	//r.RaftLog.entries = append(r.RaftLog.entries, noop)

	//r.broadcast()
	//r.updateCommit()

	_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{&noop}})
}

// updateCommit 更新commitIndex
// reference: https://github.com/RinChanNOWWW/tinykv-impl/blob/master/raft/raft.go#L791
func (r *Raft) updateCommit() {
	commitUpdate := false
	log.Infof("raft %d try to update commit %d lastindex %d", r.id, r.RaftLog.committed, r.RaftLog.LastIndex())
	//for i, p := range r.Prs {
	//	log.Infof("raft %d prs %d match %d next %d", r.id, i, p.Match, p.Next)
	//}
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		matchCount := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				matchCount++
			}
		}

		// leader only commit on it's current term (5.4.2)
		term, _ := r.RaftLog.Term(i)
		if matchCount > len(r.Prs)/2 && term == r.Term {
			r.RaftLog.committed = i
			commitUpdate = true
		}
	}

	// The tests assume that once the leader advances its commit index,
	// it will broadcast the commit index by MessageType_MsgAppend messages.
	// https://github.com/talent-plan/tinykv/pull/302
	if commitUpdate {
		log.Infof("raft %d ,updateCommit %d", r.id, r.RaftLog.committed)
		r.broadcast()
	}
}

func StepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.RequestVote()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.HandleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.becomeCandidate()
		r.RequestVote()
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func StepCandidate(r *Raft, m pb.Message) error {
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
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func StepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.HandleMsgPropose(m)
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
	case pb.MessageType_MsgHeartbeatResponse:
		r.HandleHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.HandleAppendResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.HandleTransferLeader(m)
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		return StepFollower(r, m)
	case StateCandidate:
		return StepCandidate(r, m)
	case StateLeader:
		return StepLeader(r, m)
	}
	return nil
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("raft %d addNode %d", r.id, id)
	if id == r.id {
		r.Prs[id] = &Progress{r.RaftLog.LastIndex(), r.RaftLog.LastIndex() + 1}
	} else {
		r.Prs[id] = &Progress{0, r.RaftLog.LastIndex() + 1}
	}

	if r.State == StateLeader {
		log.Infof("raft %d send heartbeat to %d", r.id, id)
		r.sendHeartbeat(id)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			log.Warn("r.peers", r.Prs)
			r.updateCommit()
		}
	}
}
