
set -e

echo "Starting index_node shard 0 on port 8001..."
python3 index_node/index_node.py 8001 &
N1=$!

echo "Starting index_node shard 1 on port 8002..."
python3 index_node/index_node.py 8002 &
N2=$!

echo "Starting index_node shard 2 on port 8003..."
python3 index_node/index_node.py 8003 &
N3=$!

sleep 1.5

echo "Starting gateway on port 8000..."
python3 gateway/gateway.py &
GATEWAY_PID=$!

cleanup() {
  echo ""
  echo "Shutting down index nodes and gateway..."
  kill "$N1" "$N2" "$N3" "$GATEWAY_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1.5

echo ""
echo "All services running. Starting client (type your query when prompted):"
echo ""
python3 client/client.py

echo ""
echo "Client exited. Background services will now be stopped."