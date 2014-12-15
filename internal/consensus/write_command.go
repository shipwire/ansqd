package consensus

import (
	"github.com/goraft/raft"
	"github.com/shipwire/ansqd/internal/consensus/db"
)

// This command writes a value to a key.
type WriteCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Creates a new write command.
func NewWriteCommand(key string, value string) *WriteCommand {
	return &WriteCommand{
		Key:   key,
		Value: value,
	}
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
	return "write"
}

// Writes a value to a key.
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*db.DB)
	db.Put(c.Key, c.Value)
	return nil, nil
}

type RecoverCommand struct {
	Host string `json:"host"`
}
