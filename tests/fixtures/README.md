# Fixture provenance

This directory contains observed provider payloads. Artificial edge cases are constructed only
inside automated tests and are never embedded or run as application demo data.

`pumpportal-create-observed.jsonl` contains four text frames captured from the free
PumpPortal create/migration subscriptions during a 20-second probe on 2026-10-26 UTC
(2026-10-25 Eastern). No API key or paid subscription was used. JSONL adds a newline
between original WebSocket payloads; the payload bytes were not JSON-reencoded.
It includes two subscription responses and two create events; one optional
`vSolInBondingCurve` is `30.987654319999976`, which is not an exact lamport quantity.
Treat that reserve as unusable, preserve the create, and never round it into volume.
This fixture says nothing about the live trade/migration schema or completeness.

SHA-256: `1b70971a8aad6320adb9170db01f9f840a3908486d9e1993b21ec63bfd211f32`.

`pumpportal-create-zstd-0133.zst` is exactly the JSONL above, compressed at level 3 with
`zstd` 0.13.3 on 2026-10-27. It is used only by a backward-compatibility test to confirm that
the patched decoder reads existing compressed provider data byte for byte. It is not another
provider observation, synthetic trade stream or application demo.

Compressed SHA-256: `a8c02032b100427f7519a5a40ef948bb3a86ceb65e7ede0ead5761495ee8a13d`.
