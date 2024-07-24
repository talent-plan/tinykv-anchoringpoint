package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter    engine_util.DBIterator
	txn     *MvccTxn
	nextKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C)

	iter := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		iter:    iter,
		txn:     txn,
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.iter
	for {
		if !iter.Valid() {
			return nil, nil, nil
		}

		iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))

		if !iter.Valid() {
			return nil, nil, nil
		}

		currentKey := DecodeUserKey(iter.Item().Key())

		if !bytes.Equal(scan.nextKey, currentKey) {
			scan.nextKey = currentKey
			continue
		}

		val, err := iter.Item().Value()
		if err != nil {
			return currentKey, nil, err
		}

		scan.advanceIter()

		write, err := ParseWrite(val)
		if err != nil {
			return currentKey, nil, err
		}

		if write == nil || write.Kind == WriteKindDelete {
			continue
		}

		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(currentKey, write.StartTS))
		if err != nil {
			return currentKey, nil, err
		}
		if value == nil {
			continue
		}

		return currentKey, value, nil
	}
}

func (scan *Scanner) advanceIter() {
	iter := scan.iter

	for {
		iter.Next()
		if !iter.Valid() {
			break
		}

		nextKey := DecodeUserKey(iter.Item().Key())
		if !bytes.Equal(scan.nextKey, nextKey) {
			scan.nextKey = nextKey
			break
		}
	}
}
