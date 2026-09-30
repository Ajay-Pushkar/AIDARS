"""M10.5/M10.19: retry.py's classification and budget logic in isolation."""
from __future__ import annotations

import pytest

from aidars.distributed.models import FailureCategory
from aidars.distributed.retry import (
    DEFAULT_MAX_ATTEMPTS,
    NON_RETRYABLE_FAILURE_CATEGORIES,
    RETRYABLE_FAILURE_CATEGORIES,
    RetryBudgetExhaustedError,
    is_retryable,
    should_retry,
)


def test_every_failure_category_is_classified_exactly_once():
    """No FailureCategory member is left unclassified, and none is in
    both the retryable and non-retryable sets -- this is the concrete
    enforcement of Part 5's 'explicitly forbid except Exception: retry'
    requirement: every category must be a deliberate choice."""
    all_categories = set(FailureCategory)
    classified = RETRYABLE_FAILURE_CATEGORIES | NON_RETRYABLE_FAILURE_CATEGORIES
    assert classified == all_categories
    assert not (RETRYABLE_FAILURE_CATEGORIES & NON_RETRYABLE_FAILURE_CATEGORIES)


@pytest.mark.parametrize("category", sorted(RETRYABLE_FAILURE_CATEGORIES, key=lambda c: c.value))
def test_retryable_categories_are_retryable(category):
    assert is_retryable(category) is True


@pytest.mark.parametrize("category", sorted(NON_RETRYABLE_FAILURE_CATEGORIES, key=lambda c: c.value))
def test_non_retryable_categories_are_not_retryable(category):
    assert is_retryable(category) is False


def test_none_category_is_not_retryable():
    """An unrecognized/absent classification must never silently retry."""
    assert is_retryable(None) is False


def test_should_retry_true_within_budget_for_retryable_category():
    assert should_retry(FailureCategory.WORKER_UNAVAILABLE, attempts_used=1, max_attempts=3) is True
    assert should_retry(FailureCategory.WORKER_UNAVAILABLE, attempts_used=2, max_attempts=3) is True


def test_should_retry_false_once_budget_exhausted():
    assert should_retry(FailureCategory.WORKER_UNAVAILABLE, attempts_used=3, max_attempts=3) is False
    assert should_retry(FailureCategory.WORKER_UNAVAILABLE, attempts_used=4, max_attempts=3) is False


def test_should_retry_false_for_non_retryable_regardless_of_budget():
    assert should_retry(FailureCategory.APPLICATION_ERROR, attempts_used=1, max_attempts=10) is False


def test_should_retry_false_for_none_category():
    assert should_retry(None, attempts_used=1, max_attempts=10) is False


def test_default_max_attempts_is_finite_and_small():
    """No infinite retry loops (Part 5) -- a concrete, small, documented
    ceiling, not an unbounded or absurdly large one."""
    assert isinstance(DEFAULT_MAX_ATTEMPTS, int)
    assert 1 <= DEFAULT_MAX_ATTEMPTS <= 10


def test_retry_budget_exhausted_error_carries_diagnostic_fields():
    err = RetryBudgetExhaustedError("w-1", attempts_used=3, max_attempts=3)
    assert err.workload_id == "w-1"
    assert err.attempts_used == 3
    assert err.max_attempts == 3
    assert "w-1" in str(err)
