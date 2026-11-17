#!/bin/bash
# Start SeaweedFS S3 test environment for aiida-s3 testing
# This script starts a local S3-compatible server (SeaweedFS) for testing
# without needing Docker or external services.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="/tmp/seaweedfs-test"
LOG_DIR="$DATA_DIR/logs"
PID_DIR="$DATA_DIR/pids"

# Find SeaweedFS binary (weed)
# Priority: WEED_BIN env var -> PATH -> common locations
find_weed_bin() {
    # Check WEED_BIN environment variable
    if [ -n "$WEED_BIN" ] && [ -f "$WEED_BIN" ]; then
        echo "$WEED_BIN"
        return 0
    fi

    # Check if weed is in PATH
    if command -v weed >/dev/null 2>&1; then
        command -v weed
        return 0
    fi

    # Check common installation locations
    local common_paths=(
        "$HOME/go/bin/weed"
        "/usr/local/bin/weed"
        "/usr/bin/weed"
        "$HOME/bin/weed"
        "/opt/seaweedfs/weed"
    )
    for path in "${common_paths[@]}"; do
        if [ -f "$path" ]; then
            echo "$path"
            return 0
        fi
    done

    return 1
}

WEED_BIN="$(find_weed_bin)" || {
    echo "ERROR: SeaweedFS binary (weed) not found."
    echo ""
    echo "Please install SeaweedFS or set the WEED_BIN environment variable:"
    echo "  export WEED_BIN=/path/to/weed"
    echo ""
    echo "Install options:"
    echo "  1. Download from https://github.com/seaweedfs/seaweedfs/releases"
    echo "  2. Install with go: go install github.com/seaweedfs/seaweedfs/weed@latest"
    echo "  3. Use Docker: docker run -p 8333:8333 chrislusf/seaweedfs"
    exit 1
}

# S3 configuration
S3_PORT=8333
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"
S3_ENDPOINT="http://localhost:$S3_PORT"

# Create directories
mkdir -p "$DATA_DIR/data/master" "$DATA_DIR/data/volume" "$LOG_DIR" "$PID_DIR"

# Create S3 config
cat > "$DATA_DIR/s3.json" << 'EOF'
{
  "identities": [
    {
      "name": "minioadmin",
      "credentials": [
        {
          "accessKey": "minioadmin",
          "secretKey": "minioadmin"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "Write",
        "List",
        "Tagging",
        "Delete"
      ]
    }
  ]
}
EOF

echo "Starting SeaweedFS test environment..."
echo "Data directory: $DATA_DIR"
echo "S3 endpoint: $S3_ENDPOINT"
echo "Using weed binary: $WEED_BIN"

# Stop any existing instances
if [ -f "$PID_DIR/master.pid" ]; then
    echo "Stopping existing SeaweedFS instances..."
    "$SCRIPT_DIR/stop_s3_test_env.sh" || true
    sleep 2
fi

# Start master server
echo "Starting master server (port 9333)..."
nohup "$WEED_BIN" master \
    -port=9333 \
    -mdir="$DATA_DIR/data/master" \
    -ip=localhost \
    > "$LOG_DIR/master.log" 2>&1 &
echo $! > "$PID_DIR/master.pid"
sleep 2

# Start volume server
mkdir -p "$DATA_DIR/data/volume"
echo "Starting volume server (port 8080)..."
nohup "$WEED_BIN" volume \
    -port=8080 \
    -mserver=localhost:9333 \
    -dir="$DATA_DIR/data/volume" \
    -ip=localhost \
    -max=100 \
    > "$LOG_DIR/volume.log" 2>&1 &
echo $! > "$PID_DIR/volume.pid"
sleep 2

# Start filer
echo "Starting filer (port 8888)..."
nohup "$WEED_BIN" filer \
    -port=8888 \
    -master=localhost:9333 \
    > "$LOG_DIR/filer.log" 2>&1 &
echo $! > "$PID_DIR/filer.pid"
sleep 2

# Start S3 API server
echo "Starting S3 API server (port $S3_PORT)..."
nohup "$WEED_BIN" s3 \
    -port=$S3_PORT \
    -filer=localhost:8888 \
    -ip.bind=localhost \
    -config="$DATA_DIR/s3.json" \
    > "$LOG_DIR/s3.log" 2>&1 &
echo $! > "$PID_DIR/s3.pid"
sleep 3

# Verify S3 is running
echo ""
echo "Checking S3 endpoint..."
if curl -s "$S3_ENDPOINT/" > /dev/null 2>&1; then
    echo "S3 API is running at $S3_ENDPOINT"
else
    echo "WARNING: S3 API may not be ready yet. Check logs in $LOG_DIR"
fi

echo ""
echo "SeaweedFS test environment started!"
echo ""
echo "Environment variables for testing:"
echo "  export AIIDA_S3_MOCK_S3=False"
echo "  export AIIDA_S3_ENDPOINT_URL=$S3_ENDPOINT"
echo "  export AIIDA_S3_ACCESS_KEY_ID=$S3_ACCESS_KEY"
echo "  export AIIDA_S3_SECRET_ACCESS_KEY=$S3_SECRET_KEY"
echo ""
echo "To stop: $SCRIPT_DIR/stop_s3_test_env.sh"
