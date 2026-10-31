#!/bin/sh
# ============================================================================
# One-shot setup of consul-fleet's ACLs: the policy drakkar-fleet and the
# worker token of that policy. Safe to run again: a second run on the same
# agent finds both, rewrites the policy's rules and leaves the token alone.
#
# Environment (from docker-compose.yml):
#   CONSUL_HTTP_ADDR   the agent, http://consul-fleet:8500
#   CONSUL_HTTP_TOKEN  the management token (initial_management of the agent)
#   WORKER_TOKEN       the SecretID of the workers' token
#
# The policy holds exactly the two rules that docs/security.md gives the
# consul providers, with the harness state_prefix "drakkar".
# ============================================================================
set -eu

POLICY_NAME=drakkar-fleet
POLICY_RULES='key_prefix "drakkar/" { policy = "write" }
session_prefix "" { policy = "write" }'
# The agent answers the leader request before its ACL system is ready.
ACL_READY_TRIES=30

tries=0
until consul acl policy list >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge "$ACL_READY_TRIES" ]; then
        echo "the ACL system of $CONSUL_HTTP_ADDR is not ready after $tries tries" >&2
        consul acl policy list >&2 || true
        exit 1
    fi
    sleep 1
done

if consul acl policy read -name="$POLICY_NAME" >/dev/null 2>&1; then
    consul acl policy update -name="$POLICY_NAME" -rules="$POLICY_RULES" >/dev/null
    echo "policy $POLICY_NAME updated"
else
    consul acl policy create -name="$POLICY_NAME" \
        -description="Rights of the drakkar fleet providers (harness)" \
        -rules="$POLICY_RULES" >/dev/null
    echo "policy $POLICY_NAME created"
fi

# Output goes to /dev/null: the token commands print the SecretID.
if CONSUL_HTTP_TOKEN="$WORKER_TOKEN" consul acl token read -self >/dev/null 2>&1; then
    echo "worker token exists"
else
    consul acl token create -secret="$WORKER_TOKEN" -policy-name="$POLICY_NAME" \
        -description="drakkar fleet workers (harness)" >/dev/null
    echo "worker token created"
fi
