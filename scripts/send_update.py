"""Send one index update to the gateway.
Usage: python3 scripts/send_update.py 16 "Title" "body text"
"""
import json, os, sys, uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.protocol import request_reply

if len(sys.argv) != 4:
    sys.exit(__doc__)
doc = {"doc_id": int(sys.argv[1]), "title": sys.argv[2], "body": sys.argv[3]}
req = {"request_id": str(uuid.uuid4()), "msg_type": "update", "doc": doc}
print("[CLIENT] SENT:", req)
print("[CLIENT] RECEIVED:", json.dumps(request_reply("127.0.0.1", 8000, req, 10), indent=2))