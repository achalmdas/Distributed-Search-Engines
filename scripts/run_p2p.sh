#!/usr/bin/env bash
# Starts 4 peers (ports 9101-9104) that form a Chord ring, publishes the documents
# into the DHT, and shows the ring. The peers keep running until you press Ctrl+C.
# Run from the repo root:  bash scripts/run_p2p.sh
mkdir -p logs
PIDS=""
cleanup() {
  echo ""
  echo "Stopping all peers..."
  kill $PIDS 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting peer 9101 (creates the ring)..."
python3 p2p/peer.py 9101 > /dev/null &
PIDS="$PIDS $!"
sleep 0.8
for port in 9102 9103 9104; do
  echo "Starting peer $port (joins through 9101)..."
  python3 p2p/peer.py $port --join 9101 > /dev/null &
  PIDS="$PIDS $!"
  sleep 0.8
done
sleep 3

echo ""
echo "Publishing the documents into the DHT..."
python3 scripts/p2p_cli.py publish 9101
python3 scripts/p2p_cli.py ring 9101

echo ""
echo "The peers are running. Logs are in logs/peer-<port>.log. In another terminal you can run:"
echo "  python3 scripts/p2p_cli.py search 9101 \"distributed systems\"     (lookup routed through the ring)"
echo "  python3 scripts/p2p_cli.py lookup 9102 distributed               (see the hop path)"
echo "  python3 p2p/peer.py 9105 --join 9101                              (a peer joins)"
echo "  python3 scripts/p2p_cli.py leave 9102                             (a peer leaves)"
echo "  python3 scripts/p2p_cli.py ring 9101                              (show the ring again)"
echo ""
echo "Press Ctrl+C to stop the peers."
wait
