#!/bin/bash
set -euo pipefail
umask 077

test -s /run/secrets/console-auth
pids=()
worker=""
stop() {
    trap - EXIT TERM INT
    if [[ -n "$worker" ]]; then
        kill -TERM "$worker" 2>/dev/null || true
        wait "$worker" || true
    fi
    kill -TERM "${pids[@]}" 2>/dev/null || true
    wait || true
}
trap stop EXIT
trap 'exit 0' TERM INT

Xvfb "$DISPLAY" -screen 0 1280x800x24 -nolisten tcp &
pids+=("$!")
for _ in {1..40}; do
    xdpyinfo >/dev/null 2>&1 && break
    sleep 0.25
done
xdpyinfo >/dev/null
openbox >/dev/null 2>&1 &
pids+=("$!")
x11vnc -localhost -display "$DISPLAY" -rfbport 5900 -nopw -forever -shared -quiet &
pids+=("$!")
websockify --web=/usr/share/novnc 127.0.0.1:6080 127.0.0.1:5900 &
pids+=("$!")
nginx -c /opt/avtopro/nginx.conf -g 'daemon off;' &
pids+=("$!")
python worker.py &
worker=$!

# Restart the container if the worker, browser desktop or web gateway exits.
wait -n "${pids[@]}" "$worker"
