#!/bin/bash
# Stop SeaweedFS S3 test environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="/tmp/seaweedfs-test"
PID_DIR="$DATA_DIR/pids"

echo "Stopping SeaweedFS test environment..."

# Stop processes in reverse order
for service in s3 filer volume master; do
    if [ -f "$PID_DIR/$service.pid" ]; then
        PID=$(cat "$PID_DIR/$service.pid")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Stopping $service (PID $PID)..."
            kill "$PID" 2>/dev/null || true
            sleep 1
            # Force kill if still running
            if kill -0 "$PID" 2>/dev/null; then
                kill -9 "$PID" 2>/dev/null || true
            fi
        fi
        rm -f "$PID_DIR/$service.pid"
    fi
done

# Also kill any remaining weed processes from this test env
pkill -f "weed.*seaweedfs-test" 2>/dev/null || true

echo "SeaweedFS test environment stopped."
echo ""
echo "To clean up all data: rm -rf $DATA_DIR"
