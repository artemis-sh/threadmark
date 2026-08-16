#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8090}"
tenant="atomic-smoke-$(date +%s)-$$"
principal="user-1"
headers=(-H "x-threadmark-tenant: $tenant" -H "x-threadmark-principal: $principal")

request=$(jq -cn '{
  idempotency_key: "start-1",
  conversation: {title: "Atomic smoke", metadata: {client: "smoke"}},
  agent_ref: "agent/test",
  items: [{type: "message", role: "user", content: [{type: "input_text", text: "hello"}]}]
}')

created=$(curl --fail-with-body --silent --show-error \
  -X POST -H 'content-type: application/json' "${headers[@]}" \
  --data "$request" "$base_url/v1/turn-starts")
replayed=$(curl --fail-with-body --silent --show-error \
  -X POST -H 'content-type: application/json' "${headers[@]}" \
  --data "$request" "$base_url/v1/turn-starts")

test "$(jq -r '.replayed' <<<"$created")" = false
test "$(jq -r '.replayed' <<<"$replayed")" = true
test "$(jq -r '.conversation_id' <<<"$created")" = "$(jq -r '.conversation_id' <<<"$replayed")"
test "$(jq -r '.turn_id' <<<"$created")" = "$(jq -r '.turn_id' <<<"$replayed")"
test "$(jq -c '.item_ids' <<<"$created")" = "$(jq -c '.item_ids' <<<"$replayed")"

changed=$(jq '.items[0].content[0].text = "changed"' <<<"$request")
changed_status=$(curl --silent --show-error -o /tmp/threadmark-atomic-changed.json \
  -w '%{http_code}' -X POST -H 'content-type: application/json' "${headers[@]}" \
  --data "$changed" "$base_url/v1/turn-starts")
test "$changed_status" = 409
test "$(jq -r '.error.code' /tmp/threadmark-atomic-changed.json)" = idempotency_key_reused

conversation_id=$(jq -r '.conversation_id' <<<"$created")
active_status=$(curl --silent --show-error -o /tmp/threadmark-atomic-active.json \
  -w '%{http_code}' -X POST -H 'content-type: application/json' "${headers[@]}" \
  --data "{\"idempotency_key\":\"start-2\",\"conversation_id\":\"$conversation_id\",\"agent_ref\":\"agent/test\",\"items\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[]}]}" \
  "$base_url/v1/turn-starts")
test "$active_status" = 409
test "$(jq -r '.error.code' /tmp/threadmark-atomic-active.json)" = active_turn_exists

printf 'atomic turn smoke passed: conversation=%s turn=%s\n' \
  "$conversation_id" "$(jq -r '.turn_id' <<<"$created")"
