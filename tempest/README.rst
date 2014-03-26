Tempest - The OpenStack Integration Test Suite
==============================================

This is a set of integration tests to be run against a live OpenStack
cluster with MagnetoDB. Tempest has batteries of tests for OpenStack API
validation, Scenarios, and other specific tests useful in validating
an OpenStack deployment.

API tests are separated to three caregories:
1. Stable - for tests that should always pass. Only this tests will
   vote in gerrit gate job.
2. In progress - for tests that fall because of bug.
3. Not ready - for tests, that cover not implemented, but expected
   functionality.

Categories 2 and 3 will be run in gate job, but will not affect on test
success result.
