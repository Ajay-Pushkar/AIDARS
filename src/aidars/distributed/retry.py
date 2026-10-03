"""M10.5: explicit, intentional retry classification and budget.

No `except Exception: retry`. A failure is only retried when its
FailureCategory (models.py) is explicitly enumerated below as retryable
-- every other category, including an unrecognized/None one, is treated
as non-retryable by default (fail closed on classification, not just on
auth).
"""
from __future__ import annotations

from typing import Optional

from aidars.distributed.models import FailureCategory

# Retryable: the failure is about the ENVIRONMENT (worker, network, CAS,
# transient resource pressure), not about the workload itself -- a retry
# on a different attempt (possibly a different worker) has a genuine
# chance of succeeding.
RETRYABLE_FAILURE_CATEGORIES = frozenset({
    FailureCategory.WORKER_UNAVAILABLE,
    FailureCategory.TEMPORARY_CAS_FAILURE,
    FailureCategory.RESOURCE_EXHAUSTION,
    FailureCategory.EXECUTION_TIMEOUT,
    FailureCategory.ASSET_TRANSFER_FAILURE,
    FailureCategory.ASSET_STAGING_FAILURE,
})

# Non-retryable: the failure is about the WORKLOAD/request itself -- retrying
# without changing anything will deterministically fail again.
NON_RETRYABLE_FAILURE_CATEGORIES = frozenset({
    FailureCategory.INVALID_INPUT,
    FailureCategory.INVALID_DEPENDENCY,
    FailureCategory.MISSING_EXECUTABLE,
    FailureCategory.INVALID_WORKLOAD,
    FailureCategory.APPLICATION_ERROR,
    FailureCategory.AUTHORIZATION_FAILURE,
    FailureCategory.MALFORMED_REQUEST,
    FailureCategory.ARTIFACT_VERIFICATION_FAILURE,
    FailureCategory.EXECUTION_FAILURE,
})

# M10.5: finite retry budget -- no infinite retry loops. 3 total attempts
# (1 initial + up to 2 retries) is the same "one inline fallback" ceiling
# the pre-M10 ad-hoc retry in workload.py already used in practice, now
# made explicit, documented, and uniformly enforced for every retryable
# failure rather than only the single dispatch-exception case.
DEFAULT_MAX_ATTEMPTS = 3


def is_retryable(failure_category: Optional[FailureCategory]) -> bool:
    """Whether a failure with this category should produce a new attempt.

    An unrecognized or absent category is deliberately non-retryable:
    retrying must be an intentional, positive classification, never a
    fallback default for "we don't know what happened."
    """
    if failure_category is None:
        return False
    return failure_category in RETRYABLE_FAILURE_CATEGORIES


class RetryBudgetExhaustedError(Exception):
    """Raised (conceptually -- callers may also just check the return
    value) when a workload has used its entire retry budget and must be
    recorded as permanently FAILED rather than attempted again."""

    def __init__(self, workload_id: str, attempts_used: int, max_attempts: int) -> None:
        self.workload_id = workload_id
        self.attempts_used = attempts_used
        self.max_attempts = max_attempts
        super().__init__(
            f"Workload '{workload_id}' exhausted its retry budget "
            f"({attempts_used}/{max_attempts} attempts used)."
        )


def should_retry(
    failure_category: Optional[FailureCategory],
    attempts_used: int,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> bool:
    """True iff another attempt should be created: the failure is
    classified retryable AND the budget has room for one more attempt."""
    if not is_retryable(failure_category):
        return False
    return attempts_used < max_attempts
