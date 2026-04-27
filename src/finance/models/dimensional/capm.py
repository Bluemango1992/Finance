"""
CAPM (Capital Asset Pricing Model) implementation.
"""

import numpy as np


def calculate_capm(expected_market_return, risk_free_rate, beta):
    """
    Calculate the expected return of an asset using the CAPM formula.

    Args:
        expected_market_return (float): Expected return of the market (as a decimal, e.g., 0.08 for 8%).
        risk_free_rate (float): Risk-free rate (as a decimal, e.g., 0.02 for 2%).
        beta (float): Beta of the asset.

    Returns:
        float: Expected return of the asset.
    """
    return risk_free_rate + beta * (expected_market_return - risk_free_rate)


def estimate_beta(asset_returns, market_returns):
    """
    Estimate the beta of an asset based on historical returns.

    Args:
        asset_returns (np.ndarray): Array of asset returns.
        market_returns (np.ndarray): Array of market returns.

    Returns:
        float: Estimated beta.
    """
    covariance = np.cov(asset_returns, market_returns)[0, 1]
    market_variance = np.var(market_returns)
    return covariance / market_variance
