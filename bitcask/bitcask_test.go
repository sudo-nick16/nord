package bitcask

import (
	"bytes"
	"os"
	"testing"
)

func Test_Nord(t *testing.T) {
	dirname := ".test_nord"
	db := NewNord(dirname, NordConfig{
		MaxFileSize: 128,
	})

	tests := []struct {
		key []byte
		val []byte
	}{
		{[]byte("1"), []byte("Nikit")},
		{[]byte("2"), []byte("Nick")},
		{[]byte("3"), []byte("Gen")},
	}

	for _, test := range tests {
		db.Put(test.key, test.val)
	}

	for _, test := range tests {
		val, err := db.Get(test.key)
		if err != nil {
			t.Logf("get key: %+v", err)
			t.Fail()
		}
		if !bytes.Equal(val, test.val) {
			t.Logf("invalid value. expected: %+v, got: %+v", test.val, val)
			t.Fail()
		}
	}

	err := os.RemoveAll(dirname)
	if err != nil {
		t.Logf("failed to delete test directory - %+v", err)
		t.Fail()
	}
}
