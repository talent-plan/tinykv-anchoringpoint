package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db  *badger.DB
	dir string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)

	if err != nil {
		// Handle error appropriately.
		// For example, you might want to log the error and return nil.
		log.Fatalf("Failed to open badger database: %v", err)
		return nil
	}
	db.Close()
	return &StandAloneStorage{
		db:  db,
		dir: conf.DBPath,
	}

	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.dir
	opts.ValueDir = s.dir
	db, err := badger.Open(opts)
	s.db = db
	if err != nil {
		// Handle error appropriately.
		// For example, you might want to log the error and return nil.
		log.Fatalf("Failed to open badger database: %v", err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn  *badger.Txn
	iter []engine_util.DBIterator
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (val []byte, err error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		return nil, nil
	}
	val, err = item.ValueCopy(val)
	return
}
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	itr := engine_util.DBIterator(engine_util.NewCFIterator(cf, s.txn))
	s.iter = append(s.iter, itr)
	return itr
}
func (s *StandAloneStorageReader) Close() {
	for _, itr := range s.iter {
		if itr.Valid() {
			itr.Close()
		}
	}
	s.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			del := m.Data.(storage.Delete)
			wb.DeleteCF(del.Cf, del.Key)
		}
	}
	return wb.WriteToDB(s.db)
}
