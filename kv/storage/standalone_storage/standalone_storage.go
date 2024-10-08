package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	conf    *config.Config
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

// GetCF 从指定的列族（Column Family）中获取给定键的值。
func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	data, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return data, err
}

// IterCF 返回一个迭代器，用于遍历指定列族中的所有键值对。
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard() //释放事务相关的资源
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"
	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)
	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)
	return &StandAloneStorage{
		conf:    conf,
		engines: engines,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		txn: s.engines.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch data := modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engines.Kv, data.Cf, data.Key, data.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, data.Cf, data.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
