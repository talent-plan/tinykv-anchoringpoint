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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	electionTick    int
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
	heartbeat        map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	log.SetLevel(log.LOG_LEVEL_INFO)
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick + rand.Intn(c.ElectionTick),
		electionTick:     c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
		heartbeat:        make(map[uint64]bool),
	}

	for _, id := range c.peers {
		raft.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}

	return raft
}
func (r *Raft) SendAppend(to uint64) bool {
	return r.sendAppend(to)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	if r.Prs[to] == nil {
		return false
	}

	entries := make([]*pb.Entry, 0)

	logTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil || r.RaftLog.FirstIndex() > r.Prs[to].Next {
		// 如果pr.Next小于log的起始index，说明pr的log落后了，需要发送snapshot
		r.sendSnapshot(to)
		return false
	}
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		entry := &r.RaftLog.entries[i-r.RaftLog.FirstIndex()]
		entries = append(entries, entry)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.Prs[to].Next - 1,
		LogTerm: logTerm,
		Entries: entries,
	})
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		// if there is a pending snapshot, send that instead of the current snapshot
		snapshot = *r.RaftLog.pendingSnapshot
	} else if err != nil {
		// snapshot未准备好
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})

}
func (r *Raft) HbCount() uint64 {
	return uint64(len(r.heartbeat))
}
func (r *Raft) CanReach(i uint64) bool {
	return r.Prs[i] != nil
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.State == StateLeader {
		// 如果没有收到大多数的心跳回复，说明可能遇到了网络分区，需要重新选举
		if r.electionElapsed >= r.electionTimeout {
			r.heartbeat[r.id] = true
			hbCount := len(r.heartbeat)
			r.electionElapsed = 0
			if hbCount*2 <= len(r.Prs) {
				r.becomeFollower(r.Term, None)
				err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
				if err != nil {
					return
				}
			}
			r.heartbeat = make(map[uint64]bool)
			r.heartbeat[r.id] = true
			r.leadTransferee = None
			r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}

	} else {
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
			r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead

	r.votes = make(map[uint64]bool)
	r.leadTransferee = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.leadTransferee = None
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}

	r.updateCommittedIndex()
}

func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return r.handleMsgHup(m)
	case pb.MessageType_MsgBeat:
		return r.handleMsgBeat(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		return r.handleMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		return r.handleMsgRequestVoteResponse(m)
	case pb.MessageType_MsgPropose:
		return r.handleMsgPropose(m)
	case pb.MessageType_MsgAppendResponse:
		return r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		return r.handleMsgHeartbeatResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		return r.handleMsgTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		return r.handleMsgTimeoutNow(m)
	}
	return nil
}
func (r *Raft) handleMsgHup(m pb.Message) error {
	if r.Prs[r.id] == nil || r.State == StateLeader {
		return nil
	}
	if r.RaftLog.pendingSnapshot != nil {
		return nil
	}
	r.becomeCandidate()
	// 如果只有一个节点，直接变为leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return nil
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: term,
		})
	}
	return nil
}

func (r *Raft) handleMsgBeat(m pb.Message) error {
	if r.State == StateLeader {
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}
	}
	return nil
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Reject:  false,
	})

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 如果term小于当前term，说明是过期的消息，拒绝
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
	if r.RaftLog.pendingSnapshot != nil {
		return
	}
	// 如果index大于当前log的最大index，说明Leader的NextIndex过新，拒绝
	if m.Index > r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}
	// 如果term不匹配，说明需要减小Leader的NextIndex, 重新发送
	if term, _ := r.RaftLog.Term(m.Index); term != m.LogTerm {
		for i := r.RaftLog.FirstIndex(); i <= m.Index; i++ {
			if term, _ := r.RaftLog.Term(i); term == m.LogTerm {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Index:   i - 1,
					Reject:  true,
				})
				return
			}
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   m.Index,
			Reject:  true,
		})
		return
	}

	for _, en := range m.Entries {
		index := en.Index
		term, err := r.RaftLog.Term(index)
		if index > r.RaftLog.LastIndex() {
			// 如果index大于当前log的最大index，直接添加
			r.RaftLog.entries = append(r.RaftLog.entries, *en)
		} else if term != en.Term || err != nil {
			// 如果term不匹配，删除当前index之后的所有log
			if index < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = nil
			} else {
				r.RaftLog.entries = r.RaftLog.entries[:index-r.RaftLog.FirstIndex()]
			}
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *en)
		}
	}

	// 更新commit index
	r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	})
}

func (r *Raft) handleMsgRequestVote(m pb.Message) error {
	switch r.State {
	case StateFollower:
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
			return nil
		}
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		if r.Vote == None || r.Vote == m.From {
			term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			if (m.Index < r.RaftLog.LastIndex() && m.LogTerm == term) || m.LogTerm < term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})

				return nil
			}
			r.Vote = m.From
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			})
			return nil
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})

	case StateCandidate, StateLeader:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if term < m.LogTerm ||
			(term == m.LogTerm && r.RaftLog.LastIndex() <= m.Index) {
			if r.Vote == None || r.Vote == m.From {
				r.becomeFollower(m.Term, None)
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  false,
				})
				return nil
			}
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})

	}

	return nil
}

func (r *Raft) handleMsgRequestVoteResponse(m pb.Message) error {
	if r.State != StateCandidate {
		return nil
	}
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if m.Reject && ((m.Term > r.Term) ||
		(m.LogTerm > term) ||
		(m.LogTerm == term && m.Index > r.RaftLog.LastIndex())) {
		r.becomeFollower(m.Term, None)
		return nil
	}
	rejCount := 0
	if m.Reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		} else {
			rejCount++
		}
	}
	if 2*count > len(r.Prs) {
		r.becomeLeader()
	} else if 2*rejCount >= len(r.Prs) {
		// 如果有一半以上的节点拒绝，直接变为follower
		r.becomeFollower(r.Term, None)
	}

	return nil
}

func (r *Raft) handleMsgPropose(m pb.Message) error {
	switch r.State {
	//只有leader才能接受proposal
	case StateLeader:
		if r.leadTransferee != None {
			return ErrProposalDropped
		}

		for i := range m.Entries {
			entry := *m.Entries[i]
			entry.Term = r.Term
			entry.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.entries = append(r.RaftLog.entries, entry)
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		}
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
		}
		r.updateCommittedIndex()
	default:
		return ErrProposalDropped
	}
	return nil
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) error {
	if r.State != StateLeader {
		return nil
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return nil
	}
	r.heartbeat[m.From] = true
	if m.Reject {
		// 如果拒绝，说明NextIndex过大，需要减小
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return nil
	}
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	commit := r.RaftLog.committed
	r.updateCommittedIndex()
	// 如果有新的log被commit，需要通知其他节点
	if commit != r.RaftLog.committed {
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}

	}
	if r.leadTransferee == m.From && m.Index == r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		})
	}

	return nil
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) error {
	if r.State != StateLeader {
		return nil
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	r.heartbeat[m.From] = true

	if m.Commit < r.RaftLog.committed || r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	return nil
}

func (r *Raft) handleMsgTransferLeader(m pb.Message) error {
	if r.State != StateLeader {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTransferLeader,
			To:      r.Lead,
			From:    m.From,
			Term:    m.Term,
		})
		return nil
	}
	if r.Prs[m.From] == nil {
		return nil
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		})
	}
	return nil
}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) error {
	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	return nil
}

func (r *Raft) updateCommittedIndex() {
	matchIndexes := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		matchIndexes[i] = prs.Match
		i++
	}
	sort.Sort(matchIndexes)
	if len(matchIndexes) == 0 {
		r.RaftLog.committed = 0
		return
	}
	majorityIndex := matchIndexes[(len(matchIndexes)-1)/2]
	// 找到大多数节点的最小index
	for ; majorityIndex > r.RaftLog.committed; majorityIndex-- {
		term, _ := r.RaftLog.Term(majorityIndex)
		if term == r.Term {
			break
		}
	}
	r.RaftLog.committed = majorityIndex
}
func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
func (r *Raft) SoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	snapIndex := m.Snapshot.Metadata.Index

	if snapIndex < r.RaftLog.committed || snapIndex < r.RaftLog.FirstIndex() {
		// 说明snapshot的index过小，拒绝
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   max(r.RaftLog.committed, r.RaftLog.FirstIndex()),
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
	if r.RaftLog.pendingSnapshot != nil && snapIndex <= r.RaftLog.pendingSnapshot.Metadata.Index {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.pendingSnapshot.Metadata.Index,
			Reject:  true,
		})
		return
	}
	if len(r.RaftLog.entries) > 0 {
		if snapIndex >= r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else {
			r.RaftLog.entries = r.RaftLog.entries[snapIndex-r.RaftLog.FirstIndex()+1:]
		}
	}
	r.RaftLog.committed = snapIndex
	r.RaftLog.stabled = snapIndex
	r.RaftLog.applied = snapIndex
	snapConf := m.Snapshot.Metadata.ConfState
	if snapConf != nil {
		r.Prs = make(map[uint64]*Progress)
		for _, id := range snapConf.Nodes {
			r.Prs[id] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	snapTerm := m.Snapshot.Metadata.Term

	if r.RaftLog.LastIndex() < snapIndex {
		// 避免entries为空导致返回index,term时错误
		entry := pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Index:     snapIndex,
			Term:      snapTerm,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if r.Prs[id] != nil {
		return
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
	r.sendAppend(id)
	r.updateCommittedIndex()
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if r.Prs[id] == nil {
		return
	}
	delete(r.Prs, id)
	if id == r.Lead && r.State == StateFollower {
		r.Lead = None
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
	r.updateCommittedIndex()
}
