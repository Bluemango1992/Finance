from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


StateT = TypeVar("StateT")


@dataclass(frozen=True)
class DiscreteMarkovChain(Generic[StateT]):
    states: tuple[StateT, ...]
    transition_counts: dict[StateT, dict[StateT, int]]
    transition_probabilities: dict[StateT, dict[StateT, float]]
    initial_state: StateT

    @classmethod
    def fit(cls, observations: list[StateT] | tuple[StateT, ...]) -> "DiscreteMarkovChain[StateT]":
        sequence = list(observations)
        if len(sequence) < 2:
            raise ValueError("observations must contain at least two states")

        states = tuple(dict.fromkeys(sequence))
        counts = {state: {next_state: 0 for next_state in states} for state in states}

        for current_state, next_state in zip(sequence, sequence[1:]):
            counts[current_state][next_state] += 1

        probabilities: dict[StateT, dict[StateT, float]] = {}
        for state, next_counts in counts.items():
            total = sum(next_counts.values())
            if total == 0:
                probabilities[state] = {next_state: 0.0 for next_state in states}
                continue
            probabilities[state] = {
                next_state: count / total for next_state, count in next_counts.items()
            }

        return cls(
            states=states,
            transition_counts=counts,
            transition_probabilities=probabilities,
            initial_state=sequence[0],
        )

    def next_state_distribution(self, state: StateT) -> dict[StateT, float]:
        if state not in self.transition_probabilities:
            raise KeyError(f"Unknown state: {state!r}")
        return dict(self.transition_probabilities[state])

    def predict_next(self, state: StateT) -> StateT:
        distribution = self.next_state_distribution(state)
        return max(self.states, key=lambda next_state: (distribution[next_state], -self.states.index(next_state)))

    def transition_count(self, from_state: StateT, to_state: StateT) -> int:
        if from_state not in self.transition_counts:
            raise KeyError(f"Unknown state: {from_state!r}")
        if to_state not in self.transition_counts[from_state]:
            raise KeyError(f"Unknown state: {to_state!r}")
        return self.transition_counts[from_state][to_state]

    def generate(self, steps: int) -> list[StateT]:
        if steps < 1:
            raise ValueError("steps must be >= 1")

        path = [self.initial_state]
        current_state = self.initial_state
        for _ in range(steps - 1):
            current_state = self.predict_next(current_state)
            path.append(current_state)
        return path


def fit_discrete_markov_chain(
    observations: list[StateT] | tuple[StateT, ...],
) -> DiscreteMarkovChain[StateT]:
    return DiscreteMarkovChain.fit(observations)