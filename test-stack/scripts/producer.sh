#!/bin/bash

# Producer script that generates messages at varying rates
# This creates lag patterns that can be observed in Grafana

BOOTSTRAP_SERVER="kafka:29092"
TOPIC1="test-topic"
TOPIC2="high-volume-topic"
TOPIC3="compacted-topic"
TOPIC4="retention-test"

# Message counter
MSG_COUNT=0

# Key counter for compacted topic (reuse keys to trigger compaction)
KEY_COUNT=0
MAX_KEYS=10  # Only use 10 unique keys so compaction can occur

# Function to produce messages
produce_messages() {
    local topic=$1
    local count=$2
    local delay=$3

    for ((i=1; i<=count; i++)); do
        MSG_COUNT=$((MSG_COUNT + 1))
        echo "message-$MSG_COUNT-$(date +%s%N)" | kafka-console-producer \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $topic \
            2>/dev/null

        if [ "$delay" != "0" ]; then
            sleep $delay
        fi
    done
}

# Function to produce keyed messages for compacted topic
produce_keyed_messages() {
    local topic=$1
    local count=$2
    local delay=$3

    for ((i=1; i<=count; i++)); do
        KEY_COUNT=$(( (KEY_COUNT % MAX_KEYS) + 1 ))
        MSG_COUNT=$((MSG_COUNT + 1))
        echo "key-$KEY_COUNT:value-$MSG_COUNT-$(date +%s%N)" | kafka-console-producer \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $topic \
            --property "parse.key=true" \
            --property "key.separator=:" \
            2>/dev/null

        if [ "$delay" != "0" ]; then
            sleep $delay
        fi
    done
}

echo "Starting producer with varying message rates..."
echo "This will create lag patterns observable in Grafana"

while true; do
    # Phase 1: Steady low rate (10 msg/sec for 30 seconds)
    echo "[$(date)] Phase 1: Low rate - 10 msg/sec for 30s"
    for ((j=1; j<=30; j++)); do
        produce_messages $TOPIC1 10 0.1
    done

    # Phase 2: Burst - high volume (100 messages quickly)
    echo "[$(date)] Phase 2: Burst - 100 messages to $TOPIC1"
    produce_messages $TOPIC1 100 0.01

    # Phase 3: High volume topic burst
    echo "[$(date)] Phase 3: High volume burst - 200 messages to $TOPIC2"
    produce_messages $TOPIC2 200 0.005

    # Phase 4: Steady medium rate (20 msg/sec for 30 seconds)
    echo "[$(date)] Phase 4: Medium rate - 20 msg/sec for 30s"
    for ((j=1; j<=30; j++)); do
        produce_messages $TOPIC1 20 0.05
    done

    # Phase 5: Pause (let consumer catch up)
    echo "[$(date)] Phase 5: Pause for 20s (consumer catch-up)"
    sleep 20

    # Phase 6: Multi-topic burst
    echo "[$(date)] Phase 6: Multi-topic burst"
    produce_messages $TOPIC1 50 0.02 &
    produce_messages $TOPIC2 100 0.01 &
    wait

    # Phase 7: Compacted topic - many messages with same keys (triggers compaction)
    echo "[$(date)] Phase 7: Compacted topic - 100 keyed messages"
    produce_keyed_messages $TOPIC3 100 0.01

    # Phase 8: Retention-test topic - continuous production for retention demo
    # Topic has 60s retention, so messages older than 60s get deleted
    # This creates a scenario where consumer's committed offset falls behind low_watermark
    echo "[$(date)] Phase 8: Retention-test topic - 30 messages (will be deleted by retention)"
    produce_messages $TOPIC4 30 0.1

    echo "[$(date)] Cycle complete. Total messages produced: $MSG_COUNT"
    echo "---"
done
