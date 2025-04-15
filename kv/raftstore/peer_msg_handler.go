package raftstore

import (
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		// result中有分区的版本信息，通过版本信息过滤掉重复的请求
		result, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			log.Fatalf("saveReadyState err:%s", err)
			return
		}
		// 更新 Region 元数据
		if result != nil {
			log.Infof("%s Apply snapshot, from %v -> %v", d.Tag, result.PrevRegion, result.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			// 1. 删除旧的 Region 范围
			if len(result.Region.Peers) > 0 {
				storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
			}
			// 2. 更新 Region 信息到 storeMeta.regions
			storeMeta.setRegion(result.Region, d.peer)
			// 3. 插入新的 Region 范围
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
			storeMeta.Unlock()
		}
		// 发送消息
		d.Send(d.ctx.trans, rd.Messages)

		if len(rd.CommittedEntries) > 0 {
			KvWb := &engine_util.WriteBatch{}
			for _, entry := range rd.CommittedEntries {
				// 处理committed日志
				d.handleCommittedEntry(&entry, KvWb)
				// 可能存在remove node的情况，所以会提前返回
				if d.stopped {
					return
				}
			}

			// 应用到状态机
			applied := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			d.peerStorage.applyState.AppliedIndex = applied
			// 持久化到badger
			if err := KvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
				log.Fatalf("err:%s", err)
			}
			KvWb.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) handleCommittedEntry(entry *eraftpb.Entry, KvWb *engine_util.WriteBatch) {
	// 对于noop entry不做处理
	if entry.Data == nil {
		return
	}

	// 处理committed日志
	msg := &raft_cmdpb.RaftCmdRequest{}
	switch entry.EntryType {
	case eraftpb.EntryType_EntryNormal:
		if err := msg.Unmarshal(entry.Data); err != nil {
			log.Fatalf("err:%s", err)
		}
	case eraftpb.EntryType_EntryConfChange:
		cc := new(eraftpb.ConfChange)
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Fatalf("err:%s", err)
		}
		if err := msg.Unmarshal(cc.Context); err != nil {
			log.Fatalf("err:%s", err)
		}
	}

	switch entry.EntryType {
	case eraftpb.EntryType_EntryNormal:
		if msg.AdminRequest != nil {
			d.handleCommittedAdminEntry(entry, KvWb)
		} else {
			d.handleCommittedNormalEntry(entry, KvWb)
		}
	case eraftpb.EntryType_EntryConfChange:
		d.handleCommittedConfChangeEntry(entry, KvWb)
	}
}

func (d *peerMsgHandler) handleCommittedAdminEntry(entry *eraftpb.Entry, KvWb *engine_util.WriteBatch) {
	req := new(raft_cmdpb.RaftCmdRequest)
	err := req.Unmarshal(entry.Data)
	if err != nil {
		log.Fatalf("err:%s", err)
	}
	switch req.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compact := req.AdminRequest.GetCompactLog()
		applyState := d.peerStorage.applyState
		if compact.CompactIndex >= applyState.TruncatedState.Index {
			applyState.TruncatedState.Index = compact.CompactIndex
			applyState.TruncatedState.Term = compact.CompactTerm
			if err := KvWb.SetMeta(meta.ApplyStateKey(d.regionId), applyState); err != nil {
				log.Fatalf("err:%s", err)
			}
			d.ScheduleCompactLog(compact.CompactIndex)
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// 在propose阶段就已经处理了，没有进行committed
		log.Fatalf("peer %d handle committed transfer leader", d.PeerId())
	case raft_cmdpb.AdminCmdType_Split:
	}
}

func (d *peerMsgHandler) handleCommittedNormalEntry(entry *eraftpb.Entry, KvWb *engine_util.WriteBatch) {
	req := new(raft_cmdpb.RaftCmdRequest)
	err := req.Unmarshal(entry.Data)
	if err != nil {
		log.Fatalf("err:%s", err)
	}
	if len(req.Requests) == 0 {
		log.Infof("%s Ignore an empty entry", d.Tag)
		return
	}
	if len(req.Requests) > 1 {
		panic("More requests")
	}
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Error:       nil,
			Uuid:        nil,
			CurrentTerm: 0,
		},
		Responses:     nil,
		AdminResponse: nil,
	}
	var txn *badger.Txn

	// 一条日志里只有一个request
	request := req.Requests[0]
	log.Infof("processNormalRequest %d, type: %d", d.regionId, request.CmdType)

	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		// 在执行 Get 或 Snap 操作时，之前同一批处理的操作（Put、Delete）还存在于内存中，Get可能会脏读
		// 所以要把之前的WriteBranch写入到数据库中
		if err := KvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Fatalf("set meta error %v", err)
		}
		KvWb.MustWriteToDB(d.peerStorage.Engines.Kv)
		KvWb.Reset()

		key, cf := request.Get.Key, request.Get.Cf
		value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, cf, key)
		log.Infof("get key: %s, value: %s", key, value)
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     &raft_cmdpb.GetResponse{Value: value},
		})
	case raft_cmdpb.CmdType_Put:
		log.Infof("put key: %s, value: %s", request.Put.Key, request.Put.Value)
		key, cf := request.Put.Key, request.Put.Cf
		value := request.Put.Value
		KvWb.SetCF(cf, key, value)
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		})
		// KvWb.WriteToDB(d.peerStorage.Engines.Kv)
	case raft_cmdpb.CmdType_Delete:
		log.Infof("delete key: %s", request.Delete.Key)
		key, cf := request.Delete.Key, request.Delete.Cf
		KvWb.DeleteCF(cf, key)
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		})
	case raft_cmdpb.CmdType_Snap:
		log.Infof("applyNormalRequest %d Snap %v", d.PeerId(), d.Region())
		d.peerStorage.applyState.AppliedIndex = entry.Index
		if err := KvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Fatalf("set meta error %v", err)
		}
		KvWb.MustWriteToDB(d.peerStorage.Engines.Kv)
		KvWb.Reset()
		resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap: &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			},
		})

		// 事务确保 AppliedIndex 和快照数据同时提交，崩溃恢复后要么全部生效，要么全部回滚
		txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}

	d.processProposals(entry, resp, txn)
}

func (d *peerMsgHandler) handleCommittedConfChangeEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	cc := new(eraftpb.ConfChange)
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		log.Fatalf("err:%s", err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		log.Fatalf("err:%s", err)
	}

	changePeer := msg.AdminRequest.ChangePeer
	targetPeer := changePeer.Peer

	region := d.Region()
	// 区分版本信息，防止重复处理
	d.Region().RegionEpoch.ConfVer++

	// 检查是否存在peer
	peerExists := false
	peerIndex := 0
	for i, peer := range region.Peers {
		if peer.Id == targetPeer.Id {
			peerExists = true
			peerIndex = i
		}
	}

	// 日志commit后，更新RegionLocalState
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if peerExists {
			log.Fatalf("%s apply add node %d, but already exists", d.Tag, cc.NodeId)
			return
		}
		log.Warnf("%s apply add node %d", d.Tag, cc.NodeId)
		d.Region().Peers = append(d.Region().Peers, targetPeer)
		d.insertPeerCache(targetPeer)

	case eraftpb.ConfChangeType_RemoveNode:
		if !peerExists {
			log.Fatalf("%s apply remove node %d, but not exists", d.Tag, cc.NodeId)
			return
		}
		// 如果删除的节点是自己，销毁自己
		if cc.NodeId == d.PeerId() {
			log.Warnf("%s begin remove self %d", d.Tag, cc.NodeId)
			d.destroyPeer()
			return
		}
		log.Warnf("%s apply remove node %d", d.Tag, cc.NodeId)
		region.Peers = append(region.Peers[:peerIndex], region.Peers[peerIndex+1:]...)
		d.Region().Peers = region.Peers
		d.removePeerCache(cc.NodeId)
	}

	meta.WriteRegionState(wb, region, rspb.PeerState_Normal)

	// 更新raft节点
	d.RaftGroup.ApplyConfChange(*cc)

	resp := &raft_cmdpb.RaftCmdResponse{AdminResponse: &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{},
	}}
	d.processProposals(entry, resp, nil)
}

// 参考https://asktug.com/t/topic/
// 在处理完RaftCmdRequest之后需要给raftCMD.Callback里发送response，client才能收到leader的响应。
// Proposal 的回复处理至关重要，处理不好会出现很多的 Request timeout。
func (d *peerMsgHandler) processProposals(entry *eraftpb.Entry, response *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	// 1.只有Leader才会将上层的消息加入到proposals中；
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		// 说明当前处理的日志条目来自旧Leader，无法处理后续提案，直接返回
		if entry.Term < proposal.term {
			return
		}

		// 当前提案属于旧Leader，已过期。触发回调返回StaleCommand错误，并移除该提案。
		if entry.Term > proposal.term {
			log.Infof("%s proposal %d-%d is stale", d.Tag, proposal.index, proposal.term)
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		// 到这里 entry.Term == proposal.term

		// 说明日志条目尚未到达提案预期位置，后续提案也无法处理，直接返回。
		if entry.Index < proposal.index {
			return
		}

		// 当前提案已失效（如被跳过），触发回调并移除提案。
		if entry.Index > proposal.index {
			log.Infof("%s proposal %d-%d is stale", d.Tag, proposal.index, proposal.term)
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		// 可以根据index与term唯一确定一个raftCMD，该Index的消息在此Term达成共识
		if entry.Index == proposal.index {
			log.Infof("%s proposal %d-%d is ok", d.Tag, proposal.index, proposal.term)
			proposal.cb.Txn = txn
			proposal.cb.Done(response)
			d.proposals = d.proposals[1:]
			return
		}

		panic(fmt.Sprintf("%s proposal %d-%d not found", d.Tag, proposal.index, proposal.term))
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		// Raft 节点之间的核心通信消息，用于日志复制、选举、心跳等。
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Fatalf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		// 用于处理客户端请求，包括读、写、事务等。
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		// 定时任务消息，用于触发 Raft 的心跳或选举超时检测。
		d.onTick()
	case message.MsgTypeSplitRegion:
		// Region 分裂消息，用于将一个大的 Region 分裂成多个小的 Region。
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		// Region 大小统计消息，用于监控和触发分裂。
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		// 快照 GC 消息，用于触发快照文件的 GC。
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		// 启动消息，用于启动 Peer 的定时任务。
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeNormalRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var key []byte
	req := msg.Requests[0]
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	err := util.CheckKeyInRegion(key, d.Region())
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	log.Infof("receive client NormalRequest %d, type: %d", d.regionId, req.CmdType)

	data, err := msg.Marshal()
	if err != nil {
		log.Fatalf("err:%s", err)
	}
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
	err = d.RaftGroup.Propose(data)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, _ := msg.Marshal()
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		for i := 0; i < len(d.proposals); i++ {
			log.Errorf("propose %d-%d", d.proposals[i].index, d.proposals[i].term)
		}
		ctx, _ := msg.Marshal()
		d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    ctx,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Fatalf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Fatalf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Fatalf("isInitialized %s d.Region() %s", d.Tag, d.Region())
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	// log.Warnf("%d onRaftGCLogTick, appliedIdx: %d", d.regionId, d.peerStorage.AppliedIndex())
	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Fatalf("err:%s", err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Fatalf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Fatalf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
