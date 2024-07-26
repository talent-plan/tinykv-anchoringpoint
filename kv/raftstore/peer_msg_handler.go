package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

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
		applySnapRes, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			return
		}
		if applySnapRes != nil && !reflect.DeepEqual(applySnapRes.PrevRegion, applySnapRes.Region) {
			d.ctx.storeMeta.Lock()
			d.peerStorage.SetRegion(applySnapRes.Region)
			d.ctx.storeMeta.regions[applySnapRes.Region.Id] = applySnapRes.Region
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapRes.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRes.Region})
			d.ctx.storeMeta.Unlock()
			d.notifyHeartbeatScheduler(applySnapRes.Region, d.peer)
		}
		if len(rd.Messages) > 0 {
			voteMsgs := make([]eraftpb.Message, 0)
			for _, msg := range rd.Messages {
				if msg.MsgType == eraftpb.MessageType_MsgRequestVote || msg.MsgType == eraftpb.MessageType_MsgRequestVoteResponse {
					voteMsgs = append(voteMsgs, msg)
				}
			}
			for i := 0; i < 3; i++ {
				d.Send(d.ctx.trans, voteMsgs)
			}
			d.Send(d.ctx.trans, rd.Messages)
			d.Send(d.ctx.trans, rd.Messages)

			log.Warnf("send raft messages %v", rd.Messages)
			log.Infof(" %v", d.Region().Peers)
		}

		for _, entry := range rd.CommittedEntries {
			d.peerStorage.applyState.AppliedIndex = entry.Index

			if entry.EntryType == eraftpb.EntryType_EntryNormal {
				d.processEntry(entry)
			} else {
				d.processChangePeer(&entry)
			}

			if d.stopped {
				return
			}
			kvWB := new(engine_util.WriteBatch)

			//update and persist the apply state
			err = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				log.Panic(err)
			}
			err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				log.Panic(err)
			}
		}
		d.RaftGroup.Advance(rd)
	}
}
func (d *peerMsgHandler) processEntry(entry eraftpb.Entry) {
	if entry.EntryType != eraftpb.EntryType_EntryNormal {
		return
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		return
	}
	if msg.Header != nil {
		if msg.Header.RegionEpoch != nil {
			err := util.CheckRegionEpoch(msg, d.Region(), true)
			if err != nil {
				res := ErrResp(err)
				d.processProposals(res, &entry)
				return
			}
		}
	}
	if len(msg.Requests) > 0 {
		req := msg.Requests[0]
		var key []byte
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		}
		if key != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
			err = util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				res := ErrResp(err)
				d.processProposals(res, &entry)
				return
			}
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			d.processPut(req, &entry)
		case raft_cmdpb.CmdType_Delete:
			d.processDelete(req, &entry)
		case raft_cmdpb.CmdType_Get:
			d.processGet(req, &entry)
		case raft_cmdpb.CmdType_Snap:
			d.processSnap(req, &entry)
		}
	}
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.processCompactLog(msg.AdminRequest, &entry)
		case raft_cmdpb.AdminCmdType_Split:
			if msg.Header.RegionId != d.regionId {
				res := ErrResp(&util.ErrRegionNotFound{RegionId: msg.Header.RegionId})
				d.processProposals(res, &entry)
				return
			}
			d.processSplit(msg.AdminRequest, &entry)
		}
	}

}
func (d *peerMsgHandler) processPut(req *raft_cmdpb.Request, entry *eraftpb.Entry) {
	log.Errorf("%v processPut,req:%v", d.Tag, req.Put)
	err := engine_util.PutCF(d.peerStorage.Engines.Kv, req.Put.Cf, req.Put.Key, req.Put.Value)
	if err != nil {
		return
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			},
		},
	}
	d.processProposals(resp, entry)

}
func (d *peerMsgHandler) processDelete(req *raft_cmdpb.Request, entry *eraftpb.Entry) {
	log.Errorf("%v processDelete,req:%v", d.Tag, req.Delete)
	err := engine_util.DeleteCF(d.peerStorage.Engines.Kv, req.Delete.Cf, req.Delete.Key)
	if err != nil {
		return
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			},
		},
	}
	d.processProposals(resp, entry)
}
func (d *peerMsgHandler) processGet(req *raft_cmdpb.Request, entry *eraftpb.Entry) {
	log.Errorf("%v processGet,req:%v", d.Tag, req.Get)
	value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: value,
				},
			},
		},
	}
	d.processProposals(resp, entry)
}
func (d *peerMsgHandler) processSnap(req *raft_cmdpb.Request, entry *eraftpb.Entry) {
	log.Errorf("%v processSnap", d.Tag)
	region := new(metapb.Region)
	err := util.CloneMsg(d.Region(), region)
	if err != nil {
		panic(err)
	}

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: region}}},
	}

	d.processProposals(cmdResp, entry)
}
func (d *peerMsgHandler) processCompactLog(req *raft_cmdpb.AdminRequest, entry *eraftpb.Entry) {
	compactLog := req.GetCompactLog()
	compactIndex := compactLog.CompactIndex
	compactTerm := compactLog.CompactTerm
	if compactIndex > d.peerStorage.truncatedIndex() {
		kvWB := new(engine_util.WriteBatch)
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm
		err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
			return
		}
		err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			log.Panic(err)
			return
		}
		d.ScheduleCompactLog(compactIndex)
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		},
	}
	d.processProposals(cmdResp, entry)
}
func (d *peerMsgHandler) processSplit(req *raft_cmdpb.AdminRequest, entry *eraftpb.Entry) {
	split := req.GetSplit()
	splitKey := split.SplitKey
	region := d.Region()
	preRegion := new(metapb.Region)
	err := util.CloneMsg(region, preRegion)
	if err != nil {
		return
	}
	log.Errorf("processSplit,splitKey:%v,region:%v", splitKey, region)
	err = util.CheckKeyInRegion(splitKey, region)
	if err != nil {
		log.Errorf("key not in region, %v, %v", splitKey, region)
		res := ErrResp(err)
		d.processProposals(res, entry)
		return
	}

	if len(req.Split.NewPeerIds) != len(d.Region().Peers) {
		res := ErrResp(errors.Errorf("length of NewPeerIds != length of Peers"))
		log.Errorf("length of NewPeerIds != length of Peers, %v, %v", req.Split.NewPeerIds, d.Region().Peers)
		d.processProposals(res, entry)
		return
	}
	// Your Code Here (2B).
	// 1. Create a new region.
	newPeers := make([]*metapb.Peer, 0)
	for i, pr := range d.Region().Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      req.Split.NewPeerIds[i],
			StoreId: pr.StoreId,
		})
	}
	newRegion := &metapb.Region{
		Id:          split.NewRegionId,
		Peers:       newPeers,
		StartKey:    splitKey,
		EndKey:      region.EndKey,
		RegionEpoch: &metapb.RegionEpoch{Version: region.RegionEpoch.Version + 1, ConfVer: region.RegionEpoch.ConfVer},
	}

	d.Region().EndKey = splitKey
	d.Region().RegionEpoch.Version++
	// 2. Write the new region and update the current region.
	d.ctx.storeMeta.Lock()
	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: preRegion})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.regions[newRegion.Id] = newRegion
	d.ctx.storeMeta.regions[region.Id] = region
	d.ctx.storeMeta.Unlock()

	//3.create a new peer for the new region
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panic(err)
	}
	newPeer.peerStorage.SetRegion(newRegion)
	d.ctx.router.register(newPeer)
	startMsg := message.Msg{
		RegionID: req.Split.NewRegionId,
		Type:     message.MsgTypeStart,
	}
	err = d.ctx.router.send(req.Split.NewRegionId, startMsg)
	if err != nil {
		log.Panic(err)
	}
	// clear region size
	//d.SizeDiffHint = 0
	//d.ApproximateSize = new(uint64)
	//4. notify the heartbeat scheduler
	d.processProposals(&raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{d.Region(), newRegion},
			},
		},
	},
		entry)
	d.notifyHeartbeatScheduler(newRegion, newPeer)
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}
func (d *peerMsgHandler) processChangePeer(entry *eraftpb.Entry) {
	cc := &eraftpb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		return
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		log.Panic(err)
		return
	}
	changePeer := msg.AdminRequest.ChangePeer
	log.Errorf("%s processChangePeer,changeType:%v,peer:%v", d.Tag, changePeer.ChangeType, changePeer.Peer)
	if msg.Header != nil {
		fromEpoch := msg.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
				log.Errorf("%v", err)
				res := ErrResp(err)
				d.processProposals(res, entry)
				return
			}
		}
	}

	d.RaftGroup.ApplyConfChange(*cc)
	kvWB := new(engine_util.WriteBatch)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if !d.havePeerStoreId(changePeer.Peer.StoreId) && !d.havePeer(changePeer.Peer.Id) {
			d.Region().RegionEpoch.ConfVer++
			peer := &metapb.Peer{
				Id:      cc.NodeId,
				StoreId: changePeer.Peer.StoreId,
			}
			d.Region().Peers = append(d.Region().GetPeers(), peer)

			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

			d.insertPeerCache(peer)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
			d.ctx.storeMeta.Unlock()
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.PeerId() && !d.stopped {
			kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
			err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			log.Errorf("remove node %d", cc.NodeId)
			d.startToDestroyPeer()
			return
		} else if d.havePeer(cc.NodeId) {
			d.ctx.storeMeta.Lock()
			d.Region().RegionEpoch.ConfVer++
			region := d.Region()
			for i, p := range region.Peers {
				if p.Id == cc.NodeId {
					region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
					break
				}
			}
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			d.removePeerCache(cc.NodeId)
			d.ctx.storeMeta.Unlock()
		}
	}
	if d.stopped {
		return
	}
	err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		log.Panic(err)
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	d.processProposals(cmdResp, entry)
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func (d *peerMsgHandler) havePeer(id uint64) bool {
	for _, p := range d.Region().Peers {
		if p.Id == id {
			return true
		}
	}
	return false
}
func (d *peerMsgHandler) havePeerStoreId(id uint64) bool {
	for _, p := range d.Region().Peers {
		if p.StoreId == id {
			return true
		}
	}
	return false
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) processProposals(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry) {
	d.removeStaleProposal(entry)
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if entry.Term < p.term {
			return
		}
		if entry.Term > p.term {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return
		}
		if entry.Index < p.index {
			return
		}
		//此时entry.Term == p.term && entry.Index == p.index
		if len(resp.Responses) > 0 && resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
			//供后续ScanSnapKV使用
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		p.cb.Done(resp)
		d.proposals = d.proposals[1:]
		return
	}
}

func (d *peerMsgHandler) removeStaleProposal(entry *eraftpb.Entry) {
	for i, p := range d.proposals {
		if p.index >= entry.Index {
			d.proposals = d.proposals[i:]
			return
		} else {
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		}
	}
	d.proposals = make([]*proposal, 0)
}
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		log.Infof("%s check store id error %v", d.Tag, err)
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		log.Infof("%s leaderid%v %v is not leader, skip proposing %v", d.Tag, leaderID, leader, req)
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
		log.Infof("%s epoch not matching %v", d.Tag, errEpochNotMatching)
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
	if d.stopped {
		cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId, Leader: d.getPeerFromCache(d.LeaderId())}))
		return
	}
	for len(msg.Requests) > 0 {
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
		}
		req := msg.Requests[0]
		var key []byte
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		}
		if key != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
			err = util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				log.Errorf("%s propose raft command error %v", d.Tag, err)
				cb.Done(ErrResp(err))
				msg.Requests = msg.Requests[1:]
				continue
			}
		}
		log.Errorf("%s propose raft command %v", d.Tag, req.CmdType)
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		d.RaftGroup.Propose(data)
		msg.Requests = msg.Requests[1:]
	}
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				return
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
			d.RaftGroup.Propose(data)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			log.Errorf("TransferLeader %s propose raft command %v", d.Tag, msg.AdminRequest.TransferLeader)
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			res := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			}
			cb.Done(res)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			changeType := msg.AdminRequest.ChangePeer.ChangeType
			peer := msg.AdminRequest.ChangePeer.Peer
			if changeType == eraftpb.ConfChangeType_RemoveNode && d.Region() != nil &&
				d.RaftGroup != nil &&
				d.Region().Peers != nil {
				if len(d.Region().Peers) >= 2 && peer.Id == d.PeerId() {
					log.Errorf("ChangePeer %s propose raft command %v,len(d.Region().Peers) == 2", d.Tag, msg.AdminRequest.ChangePeer)
					var targetPeer uint64 = 0
					for _, peer := range d.Region().Peers {
						if peer.Id != d.PeerId() {
							targetPeer = peer.Id
							break
						}
					}
					if targetPeer == 0 {
						panic("This should not happen")
					}
					d.RaftGroup.TransferLeader(targetPeer)
					return
				}
				if len(d.Region().Peers) > 2 && 2*d.RaftGroup.Raft.HbCount() <= uint64(len(d.Region().Peers))+1 {
					return
				}
			}

			log.Errorf("ChangePeer %s propose raft command %v", d.Tag, msg.AdminRequest.ChangePeer)
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})

			ctx, err := msg.Marshal()
			if err != nil {
				return
			}
			cc := eraftpb.ConfChange{
				ChangeType: changeType,
				NodeId:     peer.Id,
				Context:    ctx,
			}
			err = d.RaftGroup.ProposeConfChange(cc)
			if err != nil {
				return
			}
		case raft_cmdpb.AdminCmdType_Split:
			if msg.AdminRequest.Split.SplitKey != nil {
				err = util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
				if err != nil {
					log.Errorf("Spli KeyInRegion %s propose raft command error %v", d.Tag, err)
					cb.Done(ErrResp(err))
					return
				}
			}
			if msg.Header.RegionEpoch != nil {
				err = util.CheckRegionEpoch(msg, d.Region(), true)
				if err != nil {
					log.Errorf("Split RegionEpoch%s propose raft command error %v", d.Tag, err)
					cb.Done(ErrResp(err))
					return
				}
			}
			log.Errorf("Split %s propose raft command %v", d.Tag, msg.AdminRequest.Split)
			data, err := msg.Marshal()
			if err != nil {
				return
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
			d.RaftGroup.Propose(data)

		default:

		}
	}

}

func (d *peerMsgHandler) startToDestroyPeer() {
	if d.stopped {
		return
	}
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		if targetPeer == 0 {
			panic("This should not happen")
		}

		for i := 0; i < 5; i++ {
			if d.stopped {
				return
			}
			d.RaftGroup.Raft.SendAppend(targetPeer)
			time.Sleep(100 * time.Millisecond)
		}
	}
	if d.stopped {
		return
	}
	d.destroyPeer()

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
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
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
		log.Errorf("[region %d] send message failed %v", regionID, err)
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
		log.Warnf("%s failed to validate split region %v", d.Tag, err)
		cb.Done(ErrResp(err))
		return
	}
	log.Warnf("onPrepareSplitRegion %s split region with key %s", d.Tag, splitKey)
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
		log.Error(err)
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
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
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
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
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
