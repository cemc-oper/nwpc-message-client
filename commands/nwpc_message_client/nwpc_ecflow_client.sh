#!/bin/bash
# nwpc_ecflow_client.sh for ecflow in HPC-PI on CMA.
# 1. call ecflow_client
# 2. call nwpc_message_client to send messages.
set -x
set -e

# call ecflow_client
/g1/app/apps/ecflow/4.11.1/bin/ecflow_client "$@"

# call nwpc_message_client
set +e
NWPC_MESSAGE_CLIENT_BROKER_NODE=login_a06
broker_node=$(getent hosts ${NWPC_MESSAGE_CLIENT_BROKER_NODE} | awk '{ print $1 }')
nwpc_message_client ecflow-client \
    --command-options="$*" \
    --rabbitmq-server="amqp://guest:guest@10.28.32.114:32771" \
    --broker-address="${broker_node}:33384" \
    --with-broker
set -e

set +e
set +x