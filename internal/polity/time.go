package polity

import "github.com/hashicorp/serf/serf"

// LamportWindow is an interval of Lamport Times.
type LamportWindow struct {
	earliest, latest serf.LamportTime
}

// Witness grows l to encompass other.
func (l *LamportWindow) Witness(other serf.LamportTime) {
	if l.earliest > other {
		l.earliest = other
	}
	if l.latest < other || l.latest == 0 {
		l.latest = other
	}
}

// Before tests if t is before l.
func (l *LamportWindow) Before(t serf.LamportTime) bool {
	return t < l.earliest
}

// After tests if t is after l.
func (l *LamportWindow) After(t serf.LamportTime) bool {
	return t > l.latest
}
