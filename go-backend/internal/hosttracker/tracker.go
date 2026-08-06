package hosttracker

import (
	"fmt"

	"github.com/Genymobile/ws-scrcpy/go-backend/internal/config"
)

type Tracker struct {
	localTypes []string
	remote     []config.HostItem
}

type Message struct {
	ID   int    `json:"id"`
	Type string `json:"type"`
	Data any    `json:"data"`
}

type HostsData struct {
	Local  []LocalHost       `json:"local"`
	Remote []config.HostItem `json:"remote"`
}

type LocalHost struct {
	Type string `json:"type"`
}

func New(localTypes []string, remote []config.HostItem) *Tracker {
	localCopy := make([]string, 0, len(localTypes))
	localCopy = append(localCopy, localTypes...)
	remoteCopy := make([]config.HostItem, 0, len(remote))
	remoteCopy = append(remoteCopy, remote...)
	return &Tracker{localTypes: localCopy, remote: remoteCopy}
}

func (t *Tracker) InitialMessage() Message {
	local := make([]LocalHost, 0, len(t.localTypes))
	for _, typ := range t.localTypes {
		local = append(local, LocalHost{Type: typ})
	}
	remote := make([]config.HostItem, 0, len(t.remote))
	remote = append(remote, t.remote...)
	return Message{ID: -1, Type: "hosts", Data: HostsData{Local: local, Remote: remote}}
}

func (t *Tracker) UnsupportedMessage(input string) Message {
	return Message{ID: -1, Type: "error", Data: fmt.Sprintf("Unsupported message: %q", input)}
}
