# Latest positive gate did not meet the HTTP latency threshold

Run37998371136 on sourcefbb712d completed Java and harness checks. Real100rps60s load:
HTTP p99=197.082287ms >100ms (FAIL), E2e p99=411.99640205ms <1500ms (PASS).
HTTP errors/drops/missing payments/duplicate effects/pending after drain all0.
This is a real performance failure, not an infrastructure failure. No causal explanation
has been established; the threshold and raw measurement are preserved unchanged.
Previous green gate observations do not make this run green. No full stage-2 acceptance
is claimed while this current gate and the remaining long comparisons are unresolved.
