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
nwpc_message_client ecflow-client --common-options="$*" --broker-address="login_b01:33384"
set -e

set +e
set +x