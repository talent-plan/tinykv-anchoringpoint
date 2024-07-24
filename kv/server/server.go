package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches([][]byte{req.Key})
	server.Latches.AcquireLatches([][]byte{req.Key})
	reader, err := server.storage.Reader(req.Context)
	res := kvrpcpb.GetResponse{}
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := mvccTxn.GetLock(req.Key)
	if lock != nil {
		if lock.Ts <= req.GetVersion() {
			res.Error = &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				LockTtl:     lock.Ttl,
				Key:         req.Key,
			}}
			return &res, nil
		}
	}
	value, err := mvccTxn.GetValue(req.Key)
	if value == nil {
		res.NotFound = true
	} else {
		res.Value = value
	}
	server.Latches.ReleaseLatches([][]byte{req.Key})
	return &res, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	keys := make([][]byte, 0)
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.WaitForLatches(keys)
	server.Latches.AcquireLatches(keys)

	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	res := kvrpcpb.PrewriteResponse{}
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, m := range req.Mutations {
		write, commitTs, err := mvccTxn.MostRecentWrite(m.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs >= req.StartVersion {
			res.Errors = append(res.Errors, &kvrpcpb.KeyError{Retryable: " ", Conflict: &kvrpcpb.WriteConflict{
				StartTs:    write.StartTS,
				ConflictTs: commitTs,
				Key:        m.Key,
				Primary:    keys[0],
			}})
		}
	}
	if len(res.Errors) != 0 {
		return &res, nil
	}

	for _, m := range req.Mutations {
		lock, _ := mvccTxn.GetLock(m.Key)
		if lock != nil {
			if lock.Ts <= req.StartVersion {
				res.Errors = append(res.Errors, &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
					Key:         m.Key,
				}})

			}
		}
	}
	if len(res.Errors) == 0 {
		for _, m := range req.Mutations {
			mvccTxn.PutLock(m.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindFromProto(m.Op),
			})
			mvccTxn.PutValue(m.Key, m.Value)
		}
		err := server.storage.Write(req.Context, mvccTxn.Writes())
		if err != nil {

			return nil, err
		}
	}

	return &res, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := make([][]byte, 0)
	for _, key := range req.Keys {
		keys = append(keys, key)
	}
	server.Latches.WaitForLatches(keys)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	res := kvrpcpb.CommitResponse{}
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.CommitVersion)

	for _, key := range keys {
		lock, _ := mvccTxn.GetLock(key)
		if lock != nil {
			if lock.Ts != req.StartVersion {
				res.Error = &kvrpcpb.KeyError{Abort: " "}
				return &res, nil
			}
			mvccTxn.DeleteLock(key)
			mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Kind,
			})
		} else {
			write, _, _ := mvccTxn.MostRecentWrite(key)
			if write.Kind == mvcc.WriteKindRollback {
				res.Error = &kvrpcpb.KeyError{Abort: " "}
				return &res, nil
			}
		}
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches([][]byte{req.StartKey})
	server.Latches.AcquireLatches([][]byte{req.StartKey})
	defer server.Latches.ReleaseLatches([][]byte{req.StartKey})
	reader, err := server.storage.Reader(req.Context)
	res := kvrpcpb.ScanResponse{}
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()

		if err != nil {
			return &res, err
		}
		if key == nil {
			break
		}
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return &res, err
		}
		if lock != nil && lock.Ts < mvccTxn.StartTS {
			pair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			}
			res.Pairs = append(res.Pairs, pair)
			continue
		}
		if value == nil {
			continue
		}
		res.Pairs = append(res.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return &res, nil

}

// KvCheckTxnStatus checks for timeouts, removes expired locks and returns the status of the lock.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	res := kvrpcpb.CheckTxnStatusResponse{}
	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts != req.LockTs { //已经被提交或者回滚
		write, commitTs, _ := mvccTxn.MostRecentWrite(req.PrimaryKey)
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback { //已经被回滚
				res.CommitVersion = 0
			} else { //已经被提交
				res.CommitVersion = commitTs
			}
			res.Action = kvrpcpb.Action_NoAction
		} else { //此时primary key 已经被 commit 了，但是 secondary key 还没有被 commit
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err = server.storage.Write(req.Context, mvccTxn.Writes())
			res.Action = kvrpcpb.Action_LockNotExistRollback
		}

		return &res, err
	}
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) { //锁已经过期
		res.Action = kvrpcpb.Action_TTLExpireRollback
		mvccTxn.DeleteLock(req.PrimaryKey)
		mvccTxn.DeleteValue(req.PrimaryKey)
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, mvccTxn.Writes())
		return &res, err
	}

	return &res, nil

}

// KvBatchRollback checks that a key is locked by the current transaction, and if so removes the lock, deletes any value and leaves a rollback indicator as a write.
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	keys := make([][]byte, 0)
	for _, key := range req.Keys {
		keys = append(keys, key)
	}
	server.Latches.WaitForLatches(keys)
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	reader, err := server.storage.Reader(req.Context)
	res := kvrpcpb.BatchRollbackResponse{}
	if err != nil {
		return nil, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, _, _ := mvccTxn.MostRecentWrite(key)
			if write != nil && write.Kind == mvcc.WriteKindPut { //已经被提交
				res.Error = &kvrpcpb.KeyError{Abort: " "}
				return &res, nil
			}
			if write == nil {
				mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
			}
			continue
		}
		if lock.Ts != req.StartVersion { //锁不是当前事务的
			mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		mvccTxn.DeleteLock(key)
		mvccTxn.DeleteValue(key)
		mvccTxn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return nil, err
	}
	return &res, nil

}

// KvResolveLock inspects a batch of locked keys and either rolls them all back or commits them all.
func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	res := &kvrpcpb.ResolveLockResponse{}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)

	keys := make([][]byte, 0)
	for ; iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			return res, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return res, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		rollbackRes, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return res, err
		}
		res.Error = rollbackRes.Error
		res.RegionError = rollbackRes.RegionError
	} else {
		commitRes, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return res, err
		}
		res.Error = commitRes.Error
		res.RegionError = commitRes.RegionError
	}
	return res, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
