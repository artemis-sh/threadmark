#!/usr/bin/env bash
set -euo pipefail

base_url="${THREADMARK_URL:-http://127.0.0.1:8090}"
tenant="smoke-media-$(date +%s)"
principal="user_1"
fixture="${1:-README.md}"

headers=(
  -H "x-threadmark-tenant: $tenant"
  -H "x-threadmark-principal: $principal"
)

upload=$(curl -fsS "$base_url/v1/files" "${headers[@]}" \
  -F "file=@$fixture;type=application/octet-stream")
file_id=$(jq -r .id <<<"$upload")
file_uri=$(jq -r .uri <<<"$upload")

conversation=$(curl -fsS "$base_url/v1/conversations" "${headers[@]}" \
  -H 'content-type: application/json' \
  -d '{"title":"Media smoke test"}')
conversation_id=$(jq -r .id <<<"$conversation")

curl -fsS "$base_url/v1/conversations/$conversation_id/items" "${headers[@]}" \
  -H 'content-type: application/json' \
  -d "{\"idempotency_key\":\"media-1\",\"source\":\"user\",\"items\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_file\",\"file_url\":\"$file_uri\"}]}]}" \
  >/dev/null

replay() {
  curl -fsS "$base_url/v1/conversations/$conversation_id/replay" "${headers[@]}" \
    -H 'content-type: application/json' \
    -d "{\"file_delivery\":\"$1\"}"
}

preserved=$(replay preserve | jq -r '.input[0].content[0].file_url')
[[ "$preserved" == "$file_uri" ]]

capability_url=$(replay capability_url | jq -r '.input[0].content[0].file_url')
presigned_url=$(replay presigned_url | jq -r '.input[0].content[0].file_url')
inline_data=$(replay inline | jq -r '.input[0].content[0].file_data')
redirect_grant=$(curl -fsS "$base_url/v1/files/$file_id/downloads" "${headers[@]}" \
  -H 'content-type: application/json' -d '{"delivery":"redirect"}')
proxy_grant=$(curl -fsS "$base_url/v1/files/$file_id/downloads" "${headers[@]}" \
  -H 'content-type: application/json' -d '{"delivery":"proxy"}')

source_hash=$(sha256sum "$fixture" | cut -d ' ' -f 1)
capability_hash=$(curl -fsS "$capability_url" | sha256sum | cut -d ' ' -f 1)
presigned_hash=$(curl -fsS "$presigned_url" | sha256sum | cut -d ' ' -f 1)
inline_hash=$(base64 -d <<<"$inline_data" | sha256sum | cut -d ' ' -f 1)
redirect_hash=$(curl -fsSL "$(jq -r .url <<<"$redirect_grant")" | sha256sum | cut -d ' ' -f 1)
proxy_hash=$(curl -fsS "$(jq -r .url <<<"$proxy_grant")" | sha256sum | cut -d ' ' -f 1)

[[ "$source_hash" == "$capability_hash" ]]
[[ "$source_hash" == "$presigned_hash" ]]
[[ "$source_hash" == "$inline_hash" ]]
[[ "$source_hash" == "$redirect_hash" ]]
[[ "$source_hash" == "$proxy_hash" ]]

foreign_status=$(curl -sS -o /dev/null -w '%{http_code}' \
  "$base_url/v1/files/$file_id" \
  -H "x-threadmark-tenant: $tenant" \
  -H 'x-threadmark-principal: user_2')
[[ "$foreign_status" == "404" ]]

delete_status=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE \
  "$base_url/v1/files/$file_id" "${headers[@]}")
[[ "$delete_status" == "409" ]]

printf 'media smoke test passed (%s)\n' "$file_uri"
