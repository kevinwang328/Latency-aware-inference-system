#!/bin/bash
# start_workers.sh
# Generates gRPC stubs and starts 4 worker processes.
# Run from project root: bash start_workers.sh

set -e
cd "$(dirname "$0")"

# Generate protobuf stubs
echo "Generating gRPC stubs..."
python -m grpc_tools.protoc \
    -I system \
    --python_out=system \
    --grpc_python_out=system \
    system/inference.proto

# Fix import in generated grpc file (grpc_tools uses absolute imports)
sed -i '' 's/import inference_pb2/from . import inference_pb2/' system/inference_pb2_grpc.py 2>/dev/null || \
sed -i 's/import inference_pb2/from . import inference_pb2/' system/inference_pb2_grpc.py

echo "Starting 4 workers..."
python -m system.grpc_worker_server --worker-id 0 --port 50051 &
python -m system.grpc_worker_server --worker-id 1 --port 50052 &
python -m system.grpc_worker_server --worker-id 2 --port 50053 &
python -m system.grpc_worker_server --worker-id 3 --port 50054 &

echo "All workers started (PIDs: $!)"
echo "To stop: pkill -f grpc_worker_server"
wait
