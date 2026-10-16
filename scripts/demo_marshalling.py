"""Show a message before and after marshalling, and after unmarshalling."""
import os, sys, uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.protocol import make_message, marshal, unmarshal

msg = make_message("index_update", update_id=1,
                   doc={"doc_id": 16, "title": "Raft", "body": "Leader election and log replication."})
wire = marshal(msg)
print("1. BEFORE marshalling (Python dict):\n  ", msg)
print(f"\n2. AFTER marshalling (bytes on the wire, {len(wire)} bytes):\n  ", wire)
print("\n3. AFTER unmarshalling (receiver gets the dict back):\n  ", unmarshal(wire))
print("\n   identical to original:", unmarshal(wire) == msg)