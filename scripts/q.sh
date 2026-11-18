#!/bin/bash
# usage: scripts/q.sh "SQL"  -> prints a TSV table (runs on the jp-fsi warehouse)
databricks experimental aitools tools query "$1" --profile ${PROFILE:-jp-fsi} -o json 2>/dev/null | python3 -c '
import json,sys
raw=sys.stdin.read(); i=raw.find("[")
rows=json.loads(raw[i:]) if i>=0 else []
if not rows: print(raw); sys.exit()
cols=list(rows[0].keys())
print("\t".join(cols))
for r in rows: print("\t".join(str(r.get(c)) for c in cols))'
