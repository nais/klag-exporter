#!/bin/bash

BOOTSTRAP="kafka:29092"
NUM_TOPICS="${NUM_TOPICS:-500}"
BATCH_SIZE=50

echo "Starting bulk producer for $NUM_TOPICS topics..."

# Continuously produce messages to all topics in round-robin
CYCLE=0
while true; do
    CYCLE=$((CYCLE + 1))
    echo "[$(date)] Producer cycle $CYCLE - writing to $NUM_TOPICS topics"

    for ((i=1; i<=NUM_TOPICS; i++)); do
        # Produce 2 messages per topic per cycle using a single producer invocation
        printf "msg-${CYCLE}-1-$(date +%s%N)\nmsg-${CYCLE}-2-$(date +%s%N)\n" | \
            kafka-console-producer \
                --bootstrap-server "$BOOTSTRAP" \
                --topic "topic-$i" >> /tmp/producer.log 2>&1 &

        # Batch parallelism: wait every BATCH_SIZE topics to avoid process table overflow
        if (( i % BATCH_SIZE == 0 )); then
            wait
        fi
    done
    wait

    echo "[$(date)] Cycle $CYCLE complete - produced to all $NUM_TOPICS topics"
    sleep 5
done
