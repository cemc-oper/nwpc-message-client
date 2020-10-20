#!/bin/bash
# nwpc_ecflow_client.sh for ecFlow in HPC-PI on CMA.
# 1. call ecflow_client
# 2. call nwpc_message_client to send messages.

set -x
set -e

# call ecflow_client
/g1/app/apps/ecflow/4.11.1/bin/ecflow_client "$@"

# call nwpc_message_client

# nwpc_message_client environments, use default values if not set
export NWPC_MESSAGE_CLIENT_RABBITMQ_ADDRESS=${NWPC_MESSAGE_CLIENT_RABBITMQ_ADDRESS:-"amqp://guest:guest@locahost:9000"}

export NWPC_MESSAGE_CLIENT_BROKER_NODE=${NWPC_MESSAGE_CLIENT_BROKER_NODE:-login_b06}
export NWPC_MESSAGE_CLIENT_BROKER_PORT=${NWPC_MESSAGE_CLIENT_BROKER_PORT:-33384}

export NWPC_MESSAGE_CLINET_PROGRAM=${NWPC_MESSAGE_CLINET_PROGRAM:-nwpc_message_client@v0.5}

# send message
set +e
broker_node=$(getent hosts ${NWPC_MESSAGE_CLIENT_BROKER_NODE} | awk '{ print $1 }')
${NWPC_MESSAGE_CLINET_PROGRAM} ecflow-client \
    --command-options="$*" \
    --rabbitmq-server="${NWPC_MESSAGE_CLIENT_RABBITMQ_ADDRESS}" \
    --broker-address="${broker_node}:${NWPC_MESSAGE_CLIENT_BROKER_PORT}" \
    --with-broker
set -e

set +e
set +x