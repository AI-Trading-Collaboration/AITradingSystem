"""The deflated Sharpe ratio (Bailey and López de Prado 2014): how likely a Sharpe ratio is real
once the number of strategies tried is counted.

All Sharpe ratios here are per period (daily), not annualized, as in the paper. With N trials whose
Sharpe ratios have variance V, the best of N unskilled trials is expected to reach

    SR0 = sqrt(V) * ((1 - g) * Z(1 - 1/N) + g * Z(1 - 1/(N e)))

where Z is the standard normal quantile and g the Euler-Mascheroni constant. The deflated Sharpe
ratio is the probabilistic Sharpe ratio of the observed SR against SR0:

    PSR = Phi((SR - SR0) * sqrt(T - 1) / sqrt(1 - skew * SR + (kurt - 1) / 4 * SR^2))

with T observations and the skewness and (non-excess) kurtosis of the returns. One trial gives
SR0 = 0, so the deflated ratio equals the probabilistic ratio against zero.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from statistics import NormalDist

EULER_GAMMA = 0.5772156649015329
_NORMAL = NormalDist()


@dataclass(frozen=True)
class ReturnMoments:
    """What the deflated ratio needs from one run's daily excess returns."""

    sharpe: float
    skewness: float
    kurtosis: float
    observations: int


def expected_max_sharpe(trials: int, sharpe_variance: float) -> float:
    if trials < 1:
        raise ValueError("trials must be at least 1")
    if sharpe_variance < 0 or not math.isfinite(sharpe_variance):
        raise ValueError("sharpe_variance must be finite and not negative")
    if trials == 1:
        return 0.0
    first = _NORMAL.inv_cdf(1.0 - 1.0 / trials)
    second = _NORMAL.inv_cdf(1.0 - 1.0 / (trials * math.e))
    return math.sqrt(sharpe_variance) * ((1.0 - EULER_GAMMA) * first + EULER_GAMMA * second)


def probabilistic_sharpe(moments: ReturnMoments, benchmark_sharpe: float) -> float:
    if moments.observations < 2:
        raise ValueError("at least two observations are needed")
    sr = moments.sharpe
    spread = 1.0 - moments.skewness * sr + (moments.kurtosis - 1.0) / 4.0 * sr * sr
    if spread <= 0 or not math.isfinite(spread):
        raise ValueError("the return moments give no valid standard error")
    z = (sr - benchmark_sharpe) * math.sqrt(moments.observations - 1) / math.sqrt(spread)
    return _NORMAL.cdf(z)


def deflated_sharpe(moments: ReturnMoments, trials: int, sharpe_variance: float) -> float:
    return probabilistic_sharpe(moments, expected_max_sharpe(trials, sharpe_variance))


def sample_variance(values: Sequence[float]) -> float | None:
    """Variance of the trials' Sharpe ratios (ddof 1); None with fewer than two."""

    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    return sum((value - mean) ** 2 for value in values) / (len(values) - 1)
