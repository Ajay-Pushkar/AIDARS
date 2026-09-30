"""Session-wide test configuration.

M9: CoordinatorService is secure-by-default (fail-closed authentication)
as of this milestone. Before M9, every existing test constructed
CoordinatorService/DistributedClient/DistributedWorker with no
credentials at all, since no auth existed. Rather than touching every
one of those pre-existing test files individually, the test session
itself opts into AIDAR_INSECURE_MODE -- explicitly, visibly, in this one
file, and with zero effect on any real deployment (the env var is only
ever read inside this process). New M9 security tests that need to
exercise the real authenticated path construct their own CredentialStore
with insecure_mode=False explicitly, which always overrides this
session-wide default (CredentialStore's own `insecure_mode` constructor
argument takes precedence over the environment).
"""
import os

os.environ.setdefault("AIDAR_INSECURE_MODE", "1")
