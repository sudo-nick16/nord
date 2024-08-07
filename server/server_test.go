package server

import (
	"net/http"
	"os"
	"testing"

	"github.com/sudo-nick16/nord/bitcask"
	"github.com/sudo-nick16/nord/rafter"
)

func TestServer(t *testing.T) {
	db := bitcask.Bitcask{}
	db.Open("/tmp/.cask")
	r := rafter.New(true, &db, "/tmp/.raft", "localhost:8081")
	err := r.Open(true, "1.id")
	if err != nil {
		t.Fatalf("failed to start raft - %+v", err)
	}
	s := NewServer(r)
	err = s.ListenAndServe()
	if err != http.ErrServerClosed {
		t.Fatalf("could not start the server - %+v", err)
	}
	os.RemoveAll("/tmp/.cask")
	os.RemoveAll("/tmp/.raft")
}
