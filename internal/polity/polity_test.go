package polity

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/bradfitz/iter"
	"github.com/hashicorp/serf/command/agent"
	"github.com/hashicorp/serf/serf"
)

var port = 36320

var names = []string{
	"A",
	"B",
	"C",
	"D",
	"E",
	"F",
	"G",
	"H",
	"I",
	"J",
	"K",
	"L",
	"M",
	"N",
	"O",
	"P",
	"Q",
	"R",
	"S",
	"T",
	"U",
	"V",
	"W",
	"X",
	"Y",
	"Z",
}

func TestPolity(t *testing.T) {
	polities, ag := getAgents(t, 3)
	joinAgents(t, ag)

	err := <-polities[0].RunElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	leader, err := polities[1].QueryRole("leader")
	if err != nil {
		t.Fatal(err)
	}

	if leader != polities[0].name {
		t.Fatal(polities[0].name, "should be leader. Got", leader)
	}

	err = <-polities[1].RunRecallElection("leader")
	if err != nil {
		t.Fatal(err)
	}
}

func TestChain(t *testing.T) {
	polities, agents := getAgents(t, 7)

	for n := range agents[1:] {
		joinAgents(t, []*agent.Agent{agents[n], agents[n+1]})
	}

	err := <-polities[0].RunElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	err = <-polities[1].RunRecallElection("leader")
	if err != nil {
		t.Fatal(err)
	}

}

func TestPolityWithFailures(t *testing.T) {
	polities, agents := getAgents(t, 7)
	joinAgents(t, agents)

	err := <-polities[0].RunElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	err = <-polities[1].RunRecallElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	shutdown := rand.Perm(7)[:2]
	next := shutdown[1]
	shutdown = shutdown[:1]

	for _, n := range shutdown {
		agents[n].Shutdown()
		<-agents[n].ShutdownCh()
		fmt.Println("Shutting down node", polities[n].name)
	}

	err = <-polities[next].RunElection("leader")
	if err != nil {
		t.Fatal(err)
	}
}

func TestLostElection(t *testing.T) {
	polities, agents := getAgents(t, 7)
	joinAgents(t, agents)

	err := <-polities[0].RunElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	err = <-polities[1].RunRecallElection("leader")
	if err != nil {
		t.Fatal(err)
	}

	shutdown := rand.Perm(7)[:5]
	next := shutdown[4]
	shutdown = shutdown[:4]

	for _, n := range shutdown {
		agents[n].Shutdown()
		<-agents[n].ShutdownCh()
	}

	err = <-polities[next].RunElection("leader")
	if err != ErrLostElection {
		t.Fatal("Election should have been lost")
	}
}

func joinAgents(t *testing.T, agents []*agent.Agent) {
	addrs := make([]string, len(agents))
	for n, a := range agents {
		addrs[n] = fmt.Sprintf("%s:%d", a.Serf().LocalMember().Addr.String(), a.Serf().Memberlist().LocalNode().Port)
	}

	for n, a := range agents {
		assertJoin(t, a, addrs, names[n])
	}
}

func getAgents(t *testing.T, n int) ([]*Polity, []*agent.Agent) {
	if n > len(names) {
		t.Fatal("Insufficient names for test")
	}

	ag := make([]*agent.Agent, n)
	pl := make([]*Polity, n)
	for n := range iter.N(n) {
		a := getAgent(t, names[n])
		ag[n] = a
		pl[n] = CreateWithAgent(a)
		pl[n].name = names[n]
		pl[n].Log = log.New(os.Stderr, "", 0)
	}
	return pl, ag
}

func getAgent(t *testing.T, name string) *agent.Agent {
	agentConfig := agent.DefaultConfig()
	agentConfig.BindAddr = "127.0.0.1:0"
	agentConfig.NodeName = name

	config := serf.DefaultConfig()
	config.MemberlistConfig.BindAddr = "127.0.0.1"
	config.MemberlistConfig.BindPort = port
	config.NodeName = name

	port++

	agentA, err := agent.Create(agentConfig, config, ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}

	err = agentA.Start()
	if err != nil {
		t.Fatal(err)
	}

	return agentA
}

func assertJoin(t *testing.T, a *agent.Agent, addrs []string, name string) {
	n := len(addrs)
	joined, err := a.Join(addrs, true)
	if err != nil {
		t.Fatal(err)
	}
	if joined != n {
		t.Fatalf("%s joined %d nodes. Expected %d.", name, joined, n)
	}
}
