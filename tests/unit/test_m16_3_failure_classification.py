import pytest

from aidars.distributed.models import FailureCategory
from aidars.distributed.retry import is_retryable, should_retry

def test_m16_3_failure_classification_categories():
    """Verify M16.3 exact categories are mapped to the correct retry policy."""
    assert is_retryable(FailureCategory.EXECUTION_TIMEOUT)
    assert is_retryable(FailureCategory.ASSET_TRANSFER_FAILURE)
    assert is_retryable(FailureCategory.ASSET_STAGING_FAILURE)
    
    assert not is_retryable(FailureCategory.ARTIFACT_VERIFICATION_FAILURE)
    assert not is_retryable(FailureCategory.EXECUTION_FAILURE)
    assert not is_retryable(None)

    # Retry bounds check
    assert should_retry(FailureCategory.EXECUTION_TIMEOUT, attempts_used=1, max_attempts=3)
    assert not should_retry(FailureCategory.EXECUTION_TIMEOUT, attempts_used=3, max_attempts=3)
    
    # Non-retryable fails immediately regardless of budget
    assert not should_retry(FailureCategory.EXECUTION_FAILURE, attempts_used=1, max_attempts=3)
