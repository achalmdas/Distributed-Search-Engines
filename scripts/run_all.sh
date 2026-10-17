#!/usr/bin/env bash
# Starts the broker, 3 index shards, 3 replicas and the gateway, then runs the
# client. Run from the repo root:  bash scripts/run_all.sh
set -e
mkdir -p logs

echo "Starting broker on port 9000..."
python3 broker/broker.py > /dev/null &
BROKER=$!
sleep 0.5

for port in 8001 8002 8003; do
  echo "Starting index_node shard on port $port..."
  python3 index_node/index_node.py $port > /dev/null &
  PIDS="$PIDS $!"
done

for port in 8011 8012 8013; do
  echo "Starting replica on port $port..."
  python3 replica/replica.py $port > /dev/null &
  PIDS="$PIDS $!"
done

sleep 1.5

echo "Starting gateway on port 8000..."
python3 gateway/gateway.py > /dev/null &
GATEWAY_PID=$!

cleanup() {
  echo ""
  echo "Shutting down all services..."
  kill $BROKER $PIDS $GATEWAY_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 1.5

echo ""
echo "All services running. Logs are in logs/. In another terminal you can run:"
echo "  python3 scripts/send_update.py 16 \"Raft\" \"leader election and log replication\""
echo "  python3 scripts/status.py"
echo ""
echo "Starting client (pick 1 to search, 2 to add/update a document, 3 for sync status, 4 to quit):"
echo ""
python3 client/client.py

echo ""
echo "Client exited. Background services will now be stopped."
