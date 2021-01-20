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
	kvDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.kvDB.NewTransaction(false)
	return &MyStorageReader{
		txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, modData := range batch {
		switch m := modData.Data.(type) {
		case storage.Put:
			wb.SetCF(m.Cf, m.Key, m.Value)
		case storage.Delete:
			wb.DeleteCF(m.Cf, m.Key)
		}
	}
	return wb.WriteToDB(s.kvDB)
}

type MyStorageReader struct {
	txn *badger.Txn
}

func (m *MyStorageReader) GetCF(cf string, key []byte) ([]byte, error)  {
	byt, err := engine_util.GetCFFromTxn(m.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return byt, err
}

func (m *MyStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, m.txn)
}

func (m *MyStorageReader) Close()  {
	m.txn.Discard()
}