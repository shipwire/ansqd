package polity

import "github.com/hashicorp/serf/serf"

type LamportWindow struct {
	earliest, latest serf.LamportTime
}

func (l *LamportWindow) Witness(other serf.LamportTime) {
	if l.earliest > other {
		l.earliest = other
	}
	if l.latest < other || l.latest == 0 {
		l.latest = other
	}
}

func (l *LamportWindow) Before(t serf.LamportTime) bool {
	return t < l.earliest
}

func (l *LamportWindow) After(t serf.LamportTime) bool {
	return t > l.latest
}
