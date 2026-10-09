# Qualitative virtual-thread profile observation

Workflow37995202285 completed all9 real120s captures on source8091865. The strict collector
rejects this campaign as sustained500rps performance evidence: CPU r1 dropped742 iterations,
alloc r2 dropped23. No failed HTTP requests were observed. These outliers are preserved;
no sample is replaced and no zero-drop campaign is claimed. CPU/wall stacks may end at virtual
continuation barriers. All3 raw CPU JFR recordings observed0 VirtualThreadPinned events >=1ms;
this is an observation at the recorded load, not proof of absence under every workload.
JFR monitor stacks are exported separately. H2's accepted500rps latency measurements remain
in their original campaign and do not rely on these profiles.
