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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// firstIndex = dummyIndex + 1
	dummyIndex uint64

	// snapshotIndex 是 snapshot 的最后一个日志的 index，下一条日志为 FirstIndex
	snapshotIndex uint64
	// snapshotTerm 是 snapshotIndex 对应的 term
	snapshotTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	snapshotTerm, _ := storage.Term(firstIndex - 1)

	r := &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1, // 在 raft 中恢复 hardState
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		dummyIndex:      firstIndex - 1,
		snapshotIndex:   firstIndex - 1,
		snapshotTerm:    snapshotTerm,
		entries:         entries,
		pendingSnapshot: nil,
	}

	return r
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex, _ := l.storage.FirstIndex()
	if firstIndex > l.dummyIndex+1 && firstIndex < l.LastIndex() {
		l.entries = l.entries[firstIndex-l.dummyIndex-1:]
		l.dummyIndex = firstIndex - 1
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	unstableOffset := l.stabled - l.dummyIndex
	return l.entries[unstableOffset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	// applied是最后一个已经应用的条目索引，下一个要应用的条目是applied+1
	// 存在快照时 applied 会小于 firstIndex
	offset := max(l.applied+1, l.FirstIndex())
	// 不合法情况
	if l.committed < offset {
		return nil
	}
	ents, _ = l.Entries(offset, l.committed+1)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.snapshotIndex
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.dummyIndex + 1
}

// 检查所给日志的term与自身的term是否一致
func (l *RaftLog) matchTerm(index, term uint64) bool {
	logTerm, err := l.Term(index)
	return err == nil && logTerm == term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 1.日志为快照最后一条日志
	if i == l.snapshotIndex {
		return l.snapshotTerm, nil
	}
	// 2.日志已被压缩
	if i < l.dummyIndex {
		return 0, ErrCompacted
	}
	// 3.out of bound
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	// 4.日志在 entries 中
	offset := l.FirstIndex()
	log.Infof("offset=%d, i=%d, len(l.entries)=%d", offset, i, len(l.entries))
	return l.entries[i-offset].Term, nil
}

// 返回的是 [left, right) 的entries
func (l *RaftLog) Entries(left, right uint64) ([]pb.Entry, error) {
	if left > right {
		return nil, errors.New("invalid range")
	}
	firstIndex := l.FirstIndex()
	lastIndex := l.LastIndex()

	if left >= firstIndex && right <= lastIndex+1 {
		return l.entries[left-firstIndex : right-firstIndex], nil
	}
	return l.storage.Entries(left, right)
}

// 尝试将 Leader 的日志追加到 Follower
func (l *RaftLog) maybeAppend(index, term, commit uint64, entries []*pb.Entry) bool {
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if index > l.LastIndex() {
		log.Infof("index > LastIndex, index=%d, LastIndex=%d", index, l.LastIndex())
		return false
	}

	if !l.matchTerm(index, term) {
		log.Infof("matchTerm failed, index=%d, term=%d", index, term)
		return false
	}

	l.handleConflict(entries)

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if commit > l.committed {
		lastNewEntry := index
		if len(entries) > 0 {
			lastNewEntry = entries[len(entries)-1].Index
		}
		l.committed = min(commit, lastNewEntry)
	}

	return true
}

// 处理冲突的日志
func (l *RaftLog) handleConflict(entries []*pb.Entry) {
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 找到第一条冲突的日志
	conflictIndex := uint64(0)
	for _, v := range entries {
		if !l.matchTerm(v.Index, v.Term) {
			conflictIndex = v.Index
			break
		}
	}
	// 没有找到冲突日志，说明收到的日志以包含在当前日志中
	if conflictIndex == 0 {
		return
	}
	// 删除冲突的日志往后所有的日志
	if conflictIndex > 0 && conflictIndex <= l.LastIndex() {
		l.entries = l.entries[:conflictIndex-l.FirstIndex()]
		// 如果冲突的日志在已提交的日志之前, 则
		log.Infof("conflict l.stabled%d", l.stabled)
		l.stabled = min(l.stabled, conflictIndex-1)
	}
	// 添加新的日志
	// 原日志和新日志有冲突，或者新日志扩展了原日志，从 conflict 开始复制
	if conflictIndex > 0 {
		entries = entries[conflictIndex-entries[0].Index:]
	}
	for _, v := range entries {
		l.entries = append(l.entries, *v)
	}
}

// 将快照应用到日志
func (l *RaftLog) ApplySnap(snapshot *pb.Snapshot) {
	if l.pendingSnapshot != nil {
		return
	}
	log.Infof("apply snapshot l.stabled:%d", l.applied)
	l.pendingSnapshot = snapshot
	l.entries = nil
	l.dummyIndex = snapshot.Metadata.Index
	l.applied = snapshot.Metadata.Index
	l.stabled = snapshot.Metadata.Index

	l.snapshotIndex = snapshot.Metadata.Index
	l.snapshotTerm = snapshot.Metadata.Term
}
