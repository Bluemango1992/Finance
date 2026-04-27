import pytest

from finance.models import DiscreteMarkovChain, fit_discrete_markov_chain


def test_fit_markov_chain_builds_transition_probabilities() -> None:
    chain = fit_discrete_markov_chain(["bull", "bull", "bear", "bear", "bull", "bull"])

    assert chain.states == ("bull", "bear")
    assert chain.initial_state == "bull"
    assert chain.transition_count("bull", "bull") == 2
    assert chain.transition_count("bull", "bear") == 1
    assert chain.transition_count("bear", "bull") == 1
    assert chain.transition_count("bear", "bear") == 1
    assert chain.next_state_distribution("bull") == {"bull": pytest.approx(2 / 3), "bear": pytest.approx(1 / 3)}
    assert chain.next_state_distribution("bear") == {"bull": pytest.approx(0.5), "bear": pytest.approx(0.5)}


def test_predict_next_uses_most_likely_transition() -> None:
    chain = DiscreteMarkovChain.fit(["up", "up", "up", "down"])

    assert chain.predict_next("up") == "up"
    assert chain.generate(4) == ["up", "up", "up", "up"]


def test_unknown_state_raises_key_error() -> None:
    chain = DiscreteMarkovChain.fit(["risk_on", "risk_off", "risk_on"])

    with pytest.raises(KeyError):
        chain.next_state_distribution("neutral")


def test_fit_requires_at_least_two_observations() -> None:
    with pytest.raises(ValueError):
        fit_discrete_markov_chain(["bull"])


def test_generate_requires_positive_steps() -> None:
    chain = DiscreteMarkovChain.fit(["bull", "bear"])

    with pytest.raises(ValueError):
        chain.generate(0)