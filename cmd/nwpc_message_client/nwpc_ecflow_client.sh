#!/bin/bash
# nwpc_ecflow_client.sh
# 1. call ecflow_client
# 2. call nwpc_message_client to send messages.
set -x

# call ecflow_client
/g1/app/apps/ecflow/4.11.1/bin/ecflow_client "$@"

# call nwpc_message_client
nwpc_message_client ecflow-client --command-options="$*"

set +x