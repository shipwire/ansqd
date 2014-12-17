package polity

type QuorumFunc func(population int) (votesRequired int)

var SimpleMajority QuorumFunc = QuorumPercentage(.5, 3)

func QuorumPercentage(minimumPercent float64, minimumVotes int) QuorumFunc {
	if minimumPercent < 0 || minimumPercent > 100 {
		return nil
	}

	return func(population int) (votesRequired int) {
		votesRequired = int(float64(population)*(minimumPercent)) + 1
		if votesRequired < minimumVotes {
			return minimumVotes
		}
		return votesRequired
	}
}
