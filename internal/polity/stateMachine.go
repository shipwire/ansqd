package polity

import (
	"fmt"

	"github.com/hashicorp/serf/serf"
)

type role struct {
	node   string
	status status
	time   serf.LamportTime
}

type status int

const (
	invalid status = iota
	running
	confirmed
	impeached
	recalled
)

func (s status) String() string {
	switch s {
	case confirmed:
		return "confirmed"
	case running:
		return "running"
	case impeached:
		return "impeached"
	case recalled:
		return "recalled"
	}
	return "invalid"
}

func (s status) vacant() bool {
	return s == invalid || s == recalled
}

func (s status) eq(other status) bool {
	switch s {
	case confirmed, impeached:
		return other == confirmed || other == impeached
	case recalled, running, invalid:
		return other == running || other == recalled || other == impeached
	}
	return false
}

func (s status) confirmed(other status) bool {
	switch s {
	case impeached, recalled:
		return other == recalled
	case running, confirmed:
		return other == confirmed
	}
	return false
}

func (p *Polity) vote(q *serf.Query) {
	var err error
	var candidate, r string
	fmt.Sscan(string(q.Payload), &candidate, &r)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok && existing.status == confirmed && existing.node == candidate {
		err = q.Respond([]byte(fmt.Sprintln("YES", candidate)))
		p.roles[r] = role{candidate, running, q.LTime}
	} else if ok && !existing.status.vacant() {
		err = q.Respond([]byte(fmt.Sprintln("NO", existing.node)))
		p.logf("%s: voting NO on %s for %s because %s has role with status %s", p.name, candidate, r, existing.node, existing.status)
	} else {
		err = q.Respond([]byte(fmt.Sprintln("YES", candidate)))
		p.roles[r] = role{candidate, running, q.LTime}
	}

	if err != nil {
		p.logf("%s", err)
	}
}

func (p *Polity) confirmElection(q *serf.Query) {
	var candidate, r string
	fmt.Sscan(string(q.Payload), &candidate, &r)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	p.roles[r] = role{candidate, confirmed, q.LTime}
	err := q.Respond([]byte{})

	if err != nil {
		p.logf("%s: error confirming election: %s", p.name, err)
	}
}

func (p *Polity) voteRecall(q *serf.Query) {
	var err error
	r := string(q.Payload)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok {
		err = q.Respond([]byte(fmt.Sprintln("YES", existing.node)))
		existing.status = impeached
		existing.time = q.LTime
		p.roles[r] = existing
	} else {
		err = q.Respond([]byte(fmt.Sprintln("YES", "-")))
	}

	if err != nil {
		p.logf("%s: %s", p.name, err)
	}

}

func (p *Polity) confirmRecall(q *serf.Query) {
	r := string(q.Payload)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok {
		existing.status = recalled
		existing.time = q.LTime
		p.roles[r] = existing
	}

	q.Respond(nil)
}

func (p *Polity) query(q *serf.Query) {
	var err error
	var role string
	fmt.Sscan(string(q.Payload), &role)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[role]; ok {
		err = q.Respond([]byte(fmt.Sprintf("%s %d %d", existing.node, existing.status, existing.time)))
	} else {
		err = q.Respond([]byte(fmt.Sprintln("-", invalid, q.LTime)))
	}

	if err != nil {
		p.logf("%s", err)
	}
}
