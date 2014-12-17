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
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/serf/command/agent"
	"github.com/hashicorp/serf/serf"
)

const (
	electionBegin   = "polity.election.begin"
	electionConfirm = "polity.election.confirm"
	electionFailed  = "polity.election.fail"

	recallBegin   = "polity.recall.begin"
	recallConfirm = "polity.recall.confirm"

	updateTime = "polity.updateTime"

	query = "polity.query"

	yes = "YES"
	no  = "NO"
)

// Errors
var (
	ErrAborted      = errors.New("election aborted")
	ErrLostElection = errors.New("lost election")
	ErrRoleUnfilled = errors.New("cannot recall unfilled role")
)

// Polity represents a distributed cluster capable of electing nodes for particular roles.
type Polity struct {
	name              string
	s                 *serf.Serf
	eventCh           <-chan serf.Event
	abortConfirmation <-chan struct{}
	roles             map[string]role
	voteMutex         *sync.Mutex
	Log               *log.Logger
	QuorumFunc        QuorumFunc
}

// Create initializes a polity based on a serf instance and event channel.
func Create(s *serf.Serf, eventCh <-chan serf.Event) *Polity {
	p := &Polity{
		s:                 s,
		eventCh:           eventCh,
		roles:             make(map[string]role),
		voteMutex:         &sync.Mutex{},
		abortConfirmation: make(chan struct{}),
		QuorumFunc:        SimpleMajority,
	}

	go p.voteLoop()
	return p
}

func CreateWithAgent(a *agent.Agent) *Polity {
	eventCh := make(chan serf.Event, 10)
	p := &Polity{
		s:                 a.Serf(),
		eventCh:           eventCh,
		roles:             make(map[string]role),
		voteMutex:         &sync.Mutex{},
		abortConfirmation: make(chan struct{}),
		QuorumFunc:        SimpleMajority,
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
func (p *Polity) RunElection(role string) <-chan error {
	votesRequired := p.QuorumFunc(3)
	yesVotes := 0

	p.logf("%s running for role %s", p.name, role)

	request := fmt.Sprintf("%s %s", p.Serf().LocalMember().Name, role)
	qr, err := p.s.Query(electionBegin, []byte(request), &serf.QueryParam{Timeout: 5 * time.Second})
	if err != nil {
		return errChan(err)
	}

	for rsp := range qr.ResponseCh() {
		var vote, node string
		population := p.s.Memberlist().NumMembers()

		_, err := fmt.Sscanln(string(rsp.Payload), &vote, &node)
		if err != nil {
			p.logf("%s: error parsing response: %s: %s", p.name, err, strconv.Quote(string(rsp.Payload)))
			continue
		}

		p.logf("%s: got %s vote from %s on election", p.name, vote, rsp.From)

		if newVotesRequired := p.QuorumFunc(population); newVotesRequired > votesRequired {
			votesRequired = newVotesRequired
		}

		if vote == yes {
			yesVotes++
		}

		if yesVotes >= votesRequired {
			qr.Close()
		}
	}

	p.logf("Received %d votes. %d required", yesVotes, votesRequired)

	if yesVotes < votesRequired {
		return errChan(ErrLostElection)
	}
	return p.runConfirmation(electionConfirm, request, role, votesRequired)
}

func (p *Polity) runConfirmation(query, request, role string, votesRequired int) <-chan error {
	ch := make(chan error, 1)
	go func() {
		for {
		doConfirmation:
			confirmations := 0

			qr, err := p.s.Query(query, []byte(request), &serf.QueryParam{Timeout: 15 * time.Second})
			if err != nil {
				ch <- err
				close(ch)
				return
			}

			for {
				select {
				case <-p.abortConfirmation:
					ch <- ErrAborted
					close(ch)
					return
				case rsp := <-qr.ResponseCh():
					if rsp.From != "" {
						confirmations++
						p.logf("%s: %s confirmed %s", p.name, rsp.From, query)
					}

					if qr.Finished() && confirmations >= votesRequired {
						goto finishConfirmation
					}

					if confirmations >= p.s.Memberlist().NumMembers() {
						qr.Close()
						goto finishConfirmation
					}

				case <-time.After(50 * time.Millisecond):
					if confirmations >= p.s.Memberlist().NumMembers() {
						qr.Close()
						goto finishConfirmation
					}
					if qr.Finished() && confirmations >= votesRequired {
						goto finishConfirmation
					} else if qr.Finished() {
						goto doConfirmation
					}
				}
			}
		finishConfirmation:
			ch <- p.updateRole(role)
			close(ch)
			return
		}
	}()

	return ch
}

func errChan(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	close(ch)
	return ch
}

// RunRecallElection starts a vote to empty a role.
func (p *Polity) RunRecallElection(role string) <-chan error {
	votesRequired := 3
	yesVotes := 0

	qr, err := p.s.Query(recallBegin, []byte(role), &serf.QueryParam{Timeout: 5 * time.Second})
	if err != nil {
		return errChan(err)
	}

	for rsp := range qr.ResponseCh() {
		var vote, node string
		population := p.s.Memberlist().NumMembers()

		_, err := fmt.Sscanln(string(rsp.Payload), &vote, &node)
		if err != nil {
			p.logf("recall: %s: error parsing response: %s: %s", p.name, err, strconv.Quote(string(rsp.Payload)))
			continue
		}

		if newVotesRequired := p.QuorumFunc(population); newVotesRequired > votesRequired {
			votesRequired = newVotesRequired
		}

		if vote == yes {
			yesVotes++
		}

		if yesVotes >= votesRequired {
			qr.Close()
		}
	}

	p.logf("Received %d votes. %d required for recall", yesVotes, votesRequired)

	if yesVotes < votesRequired {
		return errChan(ErrLostElection)
	}

	return p.runConfirmation(recallConfirm, role, role, votesRequired)
}

func (p *Polity) updateRole(roleString string) error {
	r, ok := p.roles[roleString]
	if !ok {
		return nil
	}

	payload := fmt.Sprintf("%s %s %d", r.node, roleString, r.status)
	return p.s.UserEvent(updateTime, []byte(payload), false)
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
		case query:
			p.query(evt)
		case electionConfirm:
			p.confirmElection(evt)
		case recallConfirm:
			p.confirmRecall(evt)
		}
	case serf.UserEvent:
		switch evt.Name {
		case updateTime:
			p.updateTime(evt)
		}
	}
}

func (p *Polity) updateTime(q serf.UserEvent) {
	var node, r string
	var status status

	_, err := fmt.Sscanln(string(q.Payload), &node, &r, &status)
	if err != nil {
		p.logf("%s: error parsing response: %s: %s", p.name, err, strconv.Quote(string(q.Payload)))
		return
	}

	p.voteMutex.Lock()
	defer p.voteMutex.Unlock()

	if existing, ok := p.roles[r]; ok && existing.node == node && existing.status.confirmed(status) {
		existing.time = q.LTime
		existing.status = status
		p.roles[r] = existing
	} else if !ok {
		p.roles[r] = role{node: node, status: status, time: q.LTime}
	}
}

func (p *Polity) logf(f string, i ...interface{}) {
	if p.Log != nil {
		p.Log.Printf(f, i...)
	}
}

func (p *Polity) tags() map[string]string {
	return nil
}
