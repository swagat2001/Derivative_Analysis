import pytest
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
from py_vollib.black_scholes.implied_volatility import implied_volatility

def test_py_vollib_imports():
    # Verify libraries are importable and working
    # Known case: ATM Call, S=100, K=100, t=1, r=0.05, sigma=0.2
    S = 100
    K = 100
    t = 1
    r = 0.05
    sigma = 0.2
    flag = 'c'

    d = delta(flag, S, K, t, r, sigma)
    # Delta of ATM call should be roughly 0.6 (due to drift)
    assert 0.5 < d < 0.7

    v = vega(flag, S, K, t, r, sigma)
    assert v > 0

def test_implied_volatility():
    # Reverse test: allow some tolerance
    S = 100
    K = 100
    t = 1
    r = 0.05
    price = 10.45  # Approx BS price

    iv = implied_volatility(price, S, K, t, r, flag='c')
    assert 0.18 < iv < 0.22
