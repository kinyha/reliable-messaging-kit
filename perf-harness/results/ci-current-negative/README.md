# Current implementation: intentional regression rejected

Run37998420212: HTTP p99=4.616335ms (PASS), E2e p99=3912.33877728ms >1500ms (FAIL).
All other identity/error/drop/drain checks pass. 6001 committed orders and6001 payments;
zero missing or duplicate business effects. Draft PR1 differs by the intentional2s sleep.
The source checked out by the PR workflow is its merge SHA, recorded in the raw manifest.
