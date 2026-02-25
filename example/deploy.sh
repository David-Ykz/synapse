#!/bin/bash

# PATH_TO_REPO="/absolute/path/to/your/repo"
PATH_TO_REPO="/home/kzdavid/Github/synapse"

cleanup() {
    echo "Stopping all services"
    kill $(jobs -p)
    exit
}
trap cleanup SIGINT SIGTERM

SERVICES=("example/broker" "example/agents/task_allocator"  "example/user_req_handler")
# SERVICES=("example/broker" "example/agents/attractions_agent")

for DIR in "${SERVICES[@]}"; do
    FULL_PATH="$PATH_TO_REPO/$DIR"
    
    if [ -d "$FULL_PATH" ]; then
        echo "Starting service in $DIR"        
        (cd "$FULL_PATH" && go run main.go) &
    else
        echo "Directory $FULL_PATH not found, skipping."
    fi
done

wait