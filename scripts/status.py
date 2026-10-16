"""Show every replica's and shard's document count and digest (are they in sync?)."""
import json, os, sys, uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.protocol import request_reply

r = request_reply("127.0.0.1", 8000, {"request_id": str(uuid.uuid4()), "msg_type": "status"}, 10)
print(json.dumps(r, indent=2))
print("\nREPLICAS IN SYNC:", r["in_sync"])