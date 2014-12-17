package polity

// QuorumFunc is a function that determines how many votes are required to establish a voting quorum.
type QuorumFunc func(population int) (votesRequired int)

// SimpleMajority is a QuorumFunc that requires 50% + 1 nodes, with a minimum of 3.
var SimpleMajority QuorumFunc = QuorumPercentage(.5, 3)

// QuorumPercentage creates a QuorumFunc requires a minimum percentage of votes equal to or above a
// minimum number of votes.
func QuorumPercentage(minimumPercent float64, minimumVotes int) QuorumFunc {
	if minimumPercent < 0 || minimumPercent > 1 {
		panic("minimum percent must be between 0 and 1")
	}

	return func(population int) (votesRequired int) {
		votesRequired = int(float64(population)*(minimumPercent)) + 1
		if votesRequired < minimumVotes {
			return minimumVotes
		}
		return votesRequired
	}
}
