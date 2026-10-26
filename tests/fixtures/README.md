# Fixture provenance

`synthetic.jsonl` is fabricated test input, not a provider capture or on-chain transaction.
It exercises create, trade and duplicate handling with valid-shaped base58 values.
Real provider fixture evidence is recorded separately; no synthetic fixture pins a live schema.

`pumpportal-create-observed.jsonl` contains four text frames captured from the free
PumpPortal create/migration subscriptions during a 20-second probe on 2026-10-26 UTC
(2026-10-25 Eastern). No API key or paid subscription was used. JSONL adds a newline
between original WebSocket payloads; the payload bytes were not JSON-reencoded.
It includes two subscription responses and two create events; one optional
`vSolInBondingCurve` is `30.987654319999976`, which is not an exact lamport quantity.
Treat that reserve as unusable, preserve the create, and never round it into volume.
This fixture says nothing about the live trade/migration schema or completeness.

SHA-256: `1b70971a8aad6320adb9170db01f9f840a3908486d9e1993b21ec63bfd211f32`.
