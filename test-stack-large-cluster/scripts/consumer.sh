#!/bin/bash

BOOTSTRAP="kafka:29092"
NUM_TOPICS="${NUM_TOPICS:-500}"
NUM_GROUPS="${NUM_CONSUMER_GROUPS:-10}"

TOPICS_PER_GROUP=$((NUM_TOPICS / NUM_GROUPS))

echo "Starting $NUM_GROUPS consumer groups, each covering $TOPICS_PER_GROUP topics..."

# Launch one consumer per group, each subscribing to a slice of topics
for ((g=1; g<=NUM_GROUPS; g++)); do
    START=$(( (g - 1) * TOPICS_PER_GROUP + 1 ))
    END=$(( g * TOPICS_PER_GROUP ))

    # Build a regex pattern for this group's topic range
    # e.g., group 1 gets topic-1 through topic-50
    TOPIC_LIST=""
    for ((t=START; t<=END; t++)); do
        if [ -n "$TOPIC_LIST" ]; then
            TOPIC_LIST="${TOPIC_LIST}|"
        fi
        TOPIC_LIST="${TOPIC_LIST}topic-${t}"
    done

    GROUP_ID="consumer-group-${g}"
    echo "Starting $GROUP_ID for topics $START..$END"

    # Consume slowly to maintain lag
    (
        while true; do
            kafka-console-consumer \
                --bootstrap-server "$BOOTSTRAP" \
                --group "$GROUP_ID" \
                --whitelist "^(${TOPIC_LIST})$" \
                --max-messages 10 \
                --timeout-ms 30000 \
                > /dev/null 2>&1
            # Intentional delay to build lag
            sleep 15
        done
    ) &
done

echo "All $NUM_GROUPS consumer groups started."
echo "Consumers are intentionally slow to maintain lag for klag-exporter to detect."

# Keep container alive
wait
