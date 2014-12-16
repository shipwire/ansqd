/*

Package polity implements strong consensus over a gossip-based network (serf).

With a polity, you can run elections to elect the current node into a unique
role. The election will broadcast to all peers that it would like to assume a role.
If the remote node sees that role as unfilled, it will vote for the candidate and fill
the role, otherwise it will vote no.

The quorum required is (n/2)+1 where n is the maximum population known of all nodes
that voted.

A node will hold a position until it is recalled. Nodes will always vote yes to a recall,
but a quorum must still reply for the recall to succeed.

*/
package polity

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/hashicorp/serf/command/agent"
	"github.com/hashicorp/serf/serf"
)

const (
	electionBegin = "polity.election.begin"
	recallBegin   = "polity.recall.begin"

	yes = "YES"
	no  = "NO"
)

// Errors
var (
	ErrLostElection = errors.New("lost election")
	ErrRoleUnfilled = errors.New("cannot recall unfilled role")
)

// Polity represents a distributed cluster capable of electing nodes for particular roles.
type Polity struct {
	s         *serf.Serf
	eventCh   <-chan serf.Event
	roles     map[string]string
	voteMutex *sync.Mutex
	log       *log.Logger
}

// Create initializes a polity based on a serf instance and event channel.
func Create(s *serf.Serf, eventCh <-chan serf.Event) *Polity {
	p := &Polity{
		s:         s,
		eventCh:   eventCh,
		roles:     make(map[string]string),
		voteMutex: &sync.Mutex{},
	}

	go p.voteLoop()
	return p
}

func CreateWithAgent(a *agent.Agent) *Polity {
	eventCh := make(chan serf.Event, 10)
	p := &Polity{
		s:         a.Serf(),
		eventCh:   eventCh,
		roles:     make(map[string]string),
		voteMutex: &sync.Mutex{},
	}

	a.RegisterEventHandler(p.agentHandler(eventCh))

	go p.voteLoop()
	return p
}

func (p *Polity) agentHandler(eventCh chan<- serf.Event) agent.EventHandler {
	return eventHandler{eventCh}
}

type eventHandler struct {
	c chan<- serf.Event
}

func (e eventHandler) HandleEvent(s serf.Event) {
	e.c <- s
}

// Serf returns the polity's underlying serf instance.
func (p *Polity) Serf() *serf.Serf {
	return p.s
}

// RunElection initiates an election for role with the local node as the candidate.
func (p *Polity) RunElection(role string) error {
	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()
	if _, ok := p.roles[role]; ok {
		return ErrLostElection
	}

	p.roles[role] = "self"
	votesRequired := 3
	yesVotes := 1

	qr, err := p.s.Query(electionBegin, []byte(role), &serf.QueryParam{FilterTags: p.tags()})
	if err != nil {
		return err
	}

	for rsp := range qr.ResponseCh() {
		var vote, node string
		var population int

		_, err := fmt.Sscanln(string(rsp.Payload), &vote, &node, &population)
		if err != nil {
			continue
		}

		if newVotesRequired := (population / 2) + 1; newVotesRequired > votesRequired {
			votesRequired = newVotesRequired
		}

		if vote == yes {
			yesVotes++
		}
	}

	if yesVotes >= votesRequired {
		return nil
	}

	return ErrLostElection
}

// RunRecallElection starts a vote to empty a role.
func (p *Polity) RunRecallElection(role string) error {
	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()
	if _, ok := p.roles[role]; !ok {
		return ErrRoleUnfilled
	}

	votesRequired := 3
	yesVotes := 1

	qr, err := p.s.Query(recallBegin, []byte(role), &serf.QueryParam{FilterTags: p.tags()})
	if err != nil {
		return err
	}

	for rsp := range qr.ResponseCh() {
		var vote, node string
		var population int

		_, err := fmt.Sscanln(string(rsp.Payload), &vote, &node, &population)
		if err != nil {
			continue
		}

		if newVotesRequired := (population / 2) + 1; newVotesRequired > votesRequired {
			votesRequired = newVotesRequired
		}

		if vote == yes {
			yesVotes++
		}
	}

	if yesVotes >= votesRequired {
		delete(p.roles, role)
		return nil
	}

	return ErrLostElection
}

func (p *Polity) voteLoop() {
	for {
		select {
		case evt := <-p.eventCh:
			go p.handleEvent(evt)
		case <-p.s.ShutdownCh():
			return
		}
	}
}

func (p *Polity) handleEvent(e serf.Event) {
	switch evt := e.(type) {
	case *serf.Query:
		switch evt.Name {
		case electionBegin:
			p.vote(evt)
		case recallBegin:
			p.voteRecall(evt)
		}
	}
}

func (p *Polity) voteRecall(q *serf.Query) {
	var err error
	role := string(q.Payload)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[role]; ok {
		err = q.Respond([]byte(fmt.Sprint("YES", existing, len(p.s.Members()))))
	} else {
		err = q.Respond([]byte(fmt.Sprint("YES", "-", len(p.s.Members()))))
	}

	if err != nil {
		p.log.Println(err)
	}

}

func (p *Polity) vote(q *serf.Query) {
	var err error
	var candidate, role string
	fmt.Sscan(string(q.Payload), &candidate, &role)

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[role]; ok {
		err = q.Respond([]byte(fmt.Sprint("NO", existing, len(p.s.Members()))))
	} else {
		p.roles[role] = candidate
		err = q.Respond([]byte(fmt.Sprint("YES", candidate, len(p.s.Members()))))
	}

	if err != nil {
		p.log.Println(err)
	}

}

func (p *Polity) tags() map[string]string {
	return nil
}
