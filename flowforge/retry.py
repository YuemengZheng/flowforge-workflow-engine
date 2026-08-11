"""Per-node timeout and retry policy.

Lives on the node spec rather than inside a node implementation, so every node
type — including ones added later — gets the same behaviour without writing it
again. The engine applies the policy around ``Node.run``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

DEFAULT_ERROR_BRANCH = "error"


class ErrorStrategy(str, Enum):
    """What happens once a node has failed and its retries are spent.

    Three choices, the same set the reference implementation offers:

    ``FAIL``
        Stop the run. The right default — a workflow that silently carries on
        past a broken step produces confident garbage.
    ``DEFAULT``
        Substitute a canned output and keep going. For steps whose absence is
        survivable: an enrichment lookup, an optional summary.
    ``BRANCH``
        Route to an edge labelled for failure and keep going down it. For when
        the failure itself needs handling — notify, fall back, compensate.
    """

    FAIL = "fail"
    DEFAULT = "default"
    BRANCH = "branch"


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to run a node, how long to wait, and what failure means.

    ``attempts`` counts the first try, so ``attempts=1`` means no retry.
    ``timeout_s`` bounds each individual attempt, not the total.
    """

    attempts: int = 1
    timeout_s: float | None = None
    backoff_s: float = 0.2
    backoff_multiplier: float = 2.0
    max_backoff_s: float = 10.0
    on_error: ErrorStrategy = ErrorStrategy.FAIL
    error_output: Mapping[str, Any] = field(default_factory=dict)
    error_branch: str = DEFAULT_ERROR_BRANCH

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be >= 1")
        if self.timeout_s is not None and self.timeout_s <= 0:
            raise ValueError("timeout must be > 0")
        if self.backoff_s < 0:
            raise ValueError("backoff must be >= 0")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be >= 1")

    @property
    def retries(self) -> int:
        return self.attempts - 1

    @property
    def is_default(self) -> bool:
        return self.attempts == 1 and self.timeout_s is None

    def delay_before(self, next_attempt: int) -> float:
        """Exponential backoff before ``next_attempt`` (2 = the first retry)."""
        if next_attempt <= 1:
            return 0.0
        delay = self.backoff_s * (self.backoff_multiplier ** (next_attempt - 2))
        return min(delay, self.max_backoff_s)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RetryPolicy":
        """Read the node-level failure keys from a JSON node definition."""
        retries = int(data.get("retries", 0) or 0)
        timeout = data.get("timeout")
        raw_strategy = str(data.get("on_error", ErrorStrategy.FAIL.value)).lower()
        try:
            strategy = ErrorStrategy(raw_strategy)
        except ValueError:
            raise ValueError(
                f"unknown on_error {raw_strategy!r}; expected one of "
                f"{', '.join(s.value for s in ErrorStrategy)}"
            ) from None
        error_output = data.get("error_output", {})
        if not isinstance(error_output, Mapping):
            raise ValueError("'error_output' must be an object")
        return cls(
            attempts=retries + 1,
            timeout_s=None if timeout is None else float(timeout),
            backoff_s=float(data.get("retry_backoff", 0.2) or 0.0),
            backoff_multiplier=float(data.get("retry_backoff_multiplier", 2.0) or 2.0),
            on_error=strategy,
            error_output=dict(error_output),
            error_branch=str(data.get("error_branch", DEFAULT_ERROR_BRANCH)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "timeout_s": self.timeout_s,
            "backoff_s": self.backoff_s,
            "on_error": self.on_error.value,
        }


DEFAULT_RETRY = RetryPolicy()
