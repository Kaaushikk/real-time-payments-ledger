# Chaos scenarios (Toxiproxy)
#
# Ledger talks to Postgres and Redis through Toxiproxy so we can inject
# latency, resets, and timeouts without changing application code.
# Idempotency keys (Redis + Postgres) plus SERIALIZABLE retries keep
# the ledger correct across these failures.
#
# Apply (compose network):
#   curl -X POST http://localhost:8474/proxies/postgres/toxics -d @- <<'JSON'
#   {"name":"pg_latency","type":"latency","attributes":{"latency":2000}}
#   JSON
#
# Scenarios exercised in cmd/chaos:
#  1. postgres latency 2s
#  2. postgres latency 5s
#  3. postgres timeout 1s
#  4. postgres reset_peer
#  5. postgres slicer (broken packets)
#  6. postgres bandwidth 1kbps
#  7. redis latency 2s
#  8. redis timeout
#  9. redis reset_peer
# 10. redis down (proxy stop)
# 11. combined postgres+redis latency
# 12. recover (remove all toxics) and replay idempotent transfer
