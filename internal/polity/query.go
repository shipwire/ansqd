package polity

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/serf/serf"
)

// QueryRole submits a query to the cluster asking which node, if any, has a particular role.
func (p *Polity) QueryRole(role string) (string, error) {
	votesRequired := 3

	var (
		answer       string
		answerStatus status
	)

	votes := 0
	window := &LamportWindow{}

	qr, err := p.s.Query(query, []byte(role), &serf.QueryParam{Timeout: 10 * time.Second})
	if err != nil {
		return "", err
	}

	for rsp := range qr.ResponseCh() {
		var node string
		var status status
		var time serf.LamportTime
		population := p.s.Memberlist().NumMembers()

		_, err := fmt.Sscanln(string(rsp.Payload), &node, &status, &time)
		if err != nil {
			p.logf("query: %s: error parsing response: %s: %s", p.name, err, strconv.Quote(string(rsp.Payload)))
			continue
		}

		if newVotesRequired := (population / 2) + 1; newVotesRequired > votesRequired {
			votesRequired = newVotesRequired
		}

		if node == "-" {
			// this response is *really* old. disregard
			continue
		}

		if node != answer && window.Before(time) {
			// this response is old. disregard
			continue
		}

		if !status.eq(answerStatus) && window.After(time) {
			// this response is newer than what we knew. use it instead
			votes = 1
			window = &LamportWindow{}
			window.Witness(time)
			answer = node
			answerStatus = status
		}

		if node == answer {
			// this response is what we know already
			votes++
			window.Witness(time)
		}

		if votes >= votesRequired {
			return node, nil
		}
	}

	return "", ErrLostElection
}
