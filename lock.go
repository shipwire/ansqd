package main

import (
	"errors"

	"github.com/goraft/raft"
)

var (
	ErrAuditorNotFound   = errors.New("raft server does not have auditor context")
	ErrAlreadyRecovering = errors.New("host already in recovery")
	ErrDropRefused       = errors.New("host not in recovery; won't drop")
)

type RecoverLockCommand struct {
	hostname string
}

func (r RecoverLockCommand) Apply(server raft.Server) (interface{}, error) {
	a, ok := server.Context().(auditor)
	if !ok {
		return nil, ErrAuditorNotFound
	}

	host := a.GetHost(r.hostname)
	host.recoveryLock.Lock()
	defer host.recoveryLock.Unlock()
	if host.inRecovery {
		return nil, ErrAlreadyRecovering
	}

	host.inRecovery = true
	return nil, nil
}

func (d RecoverLockCommand) CommandName() string {
	return "RecoverLock"
}

type DropHostCommand struct {
	hostname string
}

func (d DropHostCommand) Apply(server raft.Server) (interface{}, error) {
	a, ok := server.Context().(auditor)
	if !ok {
		return nil, ErrAuditorNotFound
	}

	host := a.GetHost(d.hostname)
	host.recoveryLock.Lock()
	defer host.recoveryLock.Unlock()

	if !host.inRecovery {
		return nil, ErrDropRefused
	}

	a.hostsLock.Lock()
	defer a.hostsLock.Unlock()
	delete(a.hosts, d.hostname)

	return nil, nil
}

func (d DropHostCommand) CommandName() string {
	return "DropHost"
}
