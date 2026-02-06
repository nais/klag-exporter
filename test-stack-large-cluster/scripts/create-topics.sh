#!/bin/bash
set -e

BOOTSTRAP="kafka:29092"
NUM_TOPICS="${NUM_TOPICS:-500}"
PARTITIONS="${PARTITIONS_PER_TOPIC:-3}"
BATCH_SIZE=50

echo "Creating $NUM_TOPICS topics with $PARTITIONS partitions each..."

created=0
for ((i=1; i<=NUM_TOPICS; i++)); do
    kafka-topics --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "topic-$i" \
        --partitions "$PARTITIONS" \
        --replication-factor 1 &

    # Wait in batches to avoid overwhelming the broker
    if (( i % BATCH_SIZE == 0 )); then
        wait
        created=$i
        echo "Created $created / $NUM_TOPICS topics..."
    fi
done

# Wait for any remaining
wait
echo "All $NUM_TOPICS topics created successfully."

# Verify
ACTUAL=$(kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -c '^topic-' || true)
echo "Verified: $ACTUAL topics exist matching 'topic-*' pattern."
