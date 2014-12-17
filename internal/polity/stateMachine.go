package polity

import (
	"fmt"
	"strconv"

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
	confirmed
	impeached
	recalled
)

func (p *Polity) vote(q *serf.Query) {
	var err error
	var candidate, r string
	fmt.Sscan(string(q.Payload), &candidate, &r)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok && !existing.status.vacant() {
		p.logf("%s voting no", p.name)
		err = q.Respond([]byte(fmt.Sprintln("NO", existing.node, len(p.s.Members()))))
	} else {
		p.logf("%s voting yes on candidate %s for role %s", p.name, candidate, strconv.Quote(r))
		err = q.Respond([]byte(fmt.Sprintln("YES", candidate, len(p.s.Members()))))
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
	q.Respond(nil)
}

func (p *Polity) voteRecall(q *serf.Query) {
	var err error
	r := string(q.Payload)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok {
		existing.status = impeached
		existing.time = q.LTime
		p.roles[r] = existing
		err = q.Respond([]byte(fmt.Sprintln("YES", existing.node, len(p.s.Members()))))
	} else {
		err = q.Respond([]byte(fmt.Sprintln("YES", "-", len(p.s.Members()))))
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
	} else {
		p.roles[r] = role{node: "-", status: recalled, time: q.LTime}
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
		err = q.Respond([]byte(fmt.Sprintln(existing.node, existing.status, existing.time, len(p.s.Members()))))
	} else {
		err = q.Respond([]byte(fmt.Sprintln("-", invalid, q.LTime, len(p.s.Members()))))
	}

	if err != nil {
		p.logf("%s", err)
	}
}
