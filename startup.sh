#!/bin/sh
set -eu
cd /workspace
if curl -sf -o /dev/null --max-time 2 http://127.0.0.1:8080/; then
  exit 0
fi
export PYTHONPATH=/workspace/HerramientaDL_SECOP
export SECOP_PORT=8080
python3 /workspace/HerramientaDL_SECOP/app_visual/servidor_visual.py 8080 >>/tmp/app-startup.log 2>&1 &
