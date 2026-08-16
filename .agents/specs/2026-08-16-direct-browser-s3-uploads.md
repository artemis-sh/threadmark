---
title: "Direct Browser-to-S3 Uploads"
status: final
kind: rfc
created: 2026-08-16T02:27:02+00:00
---

# RFC: Direct Browser-to-S3 Uploads

## Summary

Threadmark will expose an owner-authorized, idempotent upload-session API that
lets a browser upload file bytes directly to S3-compatible storage. Threadmark
will create a pending session and return a short-lived presigned `POST` form. Once
the browser reports completion, Threadmark will inspect the staged object,
validate its size and signed metadata, copy it to an immutable final key, and
atomically create the existing finalized `files` record.

Pending uploads cannot be read, downloaded, or referenced by conversation
items. Expired and abandoned objects are removed by a durable cleanup process.
The existing `POST /v1/files` multipart endpoint remains available during the
alpha period, but direct upload is the preferred browser path.

## Motivation

The current upload endpoint calls Axum's `field.bytes()`, retaining the entire
multipart field, and then calls `bytes.to_vec()` before sending it to S3. One
upload can therefore occupy about two payload-sized buffers in Threadmark, in
addition to any buffer retained by Parley. The configured size limit is checked
only after Axum has collected the field. Concurrent uploads make this a service
availability risk and force file traffic through two application services:

```text
Browser -> Parley memory -> Threadmark memory -> S3
```

Downloads already support direct presigned S3 delivery or a streaming
Threadmark proxy. Uploads need the corresponding direct path:

```text
Browser -> Threadmark authorization and upload session -> S3
Browser -> Threadmark finalization
```

Presigning a final object key is insufficient. A presigned `PUT` can normally be
replayed until it expires, so a browser could replace bytes after Threadmark had
made the file referenceable. A pending database record alone does not prevent
that race. This RFC stages the upload at a temporary key and uses a server-side
copy to a key for which the browser has never received write authority.

## Goals

- Keep file payload bytes out of Parley and Threadmark during browser uploads.
- Preserve the invariant that every row in `files` names a complete, immutable,
  owner-scoped object that can immediately be referenced and downloaded.
- Enforce authorization, ownership, exact size, maximum size, content type,
  and upload expiration without granting S3 credentials to a browser.
- Make initiation and completion safe to retry after timeouts or process
  crashes, including a crash after S3 copy but before the database commit.
- Prevent pending or expired uploads from becoming transcript references.
- Durably clean up abandoned staging objects and untracked final copies.
- Work against AWS S3 and the project's supported S3-compatible local service.

## Non-goals

- Browser multipart upload for objects larger than the existing file limit.
- Resumable or chunked uploads.
- Malware scanning, media transcoding, or content-based MIME detection.
- Public buckets or long-lived browser S3 credentials.
- Making one transaction span PostgreSQL and S3.
- Removing the existing server-mediated multipart endpoint in this change.
- Eliminating buffering in the existing `inline` replay projection.

## Proposal

### API

Add two owner-session endpoints:

```text
POST /v1/file-uploads
POST /v1/file-uploads/{upload_id}/complete
```

Both require `file:create`. Delegated-agent tokens cannot initiate or complete
uploads. All session queries include the authenticated `tenant_id` and
`principal_id`; a session belonging to another owner returns `404`.

#### Initiate

The request is JSON:

```json
{
  "idempotency_key": "parley-upload-018",
  "filename": "diagram.png",
  "mime_type": "image/png",
  "size": 483102
}
```

Validation and normalization are:

- `idempotency_key` is trimmed and contains 1 to 200 Unicode scalar values.
- `filename` uses the existing filename normalization, is non-empty afterward,
  and is limited to 300 Unicode scalar values.
- All persisted strings reject U+0000 before any database operation.
- `mime_type` must parse as one HTTP media type without parameters or control
  characters, is serialized in normalized lowercase type/subtype form, and is
  limited to 200 ASCII bytes. Invalid values return `400`; they are not replaced
  with a default. This value is safe in a signed POST field and response header.
- `size` is an integer from zero through `FILE_MAX_MB * 1024 * 1024` inclusive.
- Unknown JSON fields are rejected to catch client mistakes.

The idempotency scope is `(tenant_id, owner_ref, client_id, idempotency_key)`,
where `client_id` is taken from the verified access token. Threadmark
stores `request_version = 1` and the lowercase hexadecimal SHA-256 digest of the
UTF-8 bytes of this RFC 8785-canonicalized JSON value, after the normalization
above:

```json
{"filename":"diagram.png","mime_type":"image/png","operation":"file_upload","size":483102,"version":1}
```

The field names and values form the object; they are not string-concatenated.
`size` is an exact nonnegative integer within the configured bound and therefore
within the interoperable JSON integer range. On replay, Threadmark loads the
stored row first and computes according to that row's `request_version`. Future
normalization or request fields require a new version and never reinterpret an
existing row. An exact retry of a `pending` session returns the original session and a
new upload form with expiration no later than the session expiration. A
`finalizing` retry returns `200` with that status, no upload form, and a
`Retry-After` value. Reuse with different input returns `409` with
`error.code = "idempotency_mismatch"`. A retry of a completed
session returns `200` with `status = "completed"` and its file, without an upload
form. A retry of an expired or `cleanup_pending` session returns `410` with
`error.code = "upload_expired"`; idempotency keys are not reusable during the
retention period. If the completed file was subsequently deleted, the retry
returns `410` with `error.code = "upload_file_deleted"`.

The `201 Created` response is:

```json
{
  "id": "upload_01...",
  "status": "pending",
  "expires_at": "2026-08-16T03:00:00Z",
  "upload": {
    "method": "POST",
    "url": "https://objects.example/...",
    "fields": {
      "key": "...",
      "Content-Type": "image/png",
      "x-amz-meta-threadmark-upload": "upload_01...",
      "policy": "...",
      "x-amz-algorithm": "AWS4-HMAC-SHA256",
      "x-amz-credential": "...",
      "x-amz-date": "...",
      "x-amz-signature": "..."
    },
    "expires_at": "2026-08-16T02:35:00Z"
  }
}
```

The browser constructs `multipart/form-data` from exactly the returned fields
and appends the file as the final form field. The POST policy requires exact
matches for the bucket, staging key, `Content-Type`, and
`x-amz-meta-threadmark-upload`, and a `content-length-range` whose lower and
upper bounds both equal the declared file size. The implementation's provider
compatibility tests must demonstrate that the policy limit applies to object
bytes, including zero-byte files; a provider for which exact object size cannot
be enforced is unsupported for direct upload. The opaque upload ID is generated
by Threadmark and the metadata value binds an object to its session. The response
does not expose a final file ID or any final object key.

The URL lifetime is `min(FILE_UPLOAD_URL_TTL_SECONDS, session remaining
lifetime)`. Its default is 60 seconds and its configured maximum is 300 seconds.
The session lifetime is fixed when first created from
`FILE_UPLOAD_SESSION_TTL_SECONDS`, defaulting to 3600 seconds; retrying initiation
does not extend it. Initiation requires `S3_PUBLIC_URL`, otherwise it returns
`503` with `error.code = "direct_upload_unavailable"`.

`size` is enforced by S3 and rechecked on completion. S3 CORS allows configured
application origins to issue `POST` and exposes only response fields needed to
detect success. Operators must not use wildcard origins. The signed policy is
the authorization; bucket public access remains disabled. API-edge limits bound
session creation, while exact-size policy enforcement prevents one session from
being reused for oversized direct requests. Provider request-rate and account
cost controls remain operational requirements because any bearer upload form can
still be replayed with valid, correctly sized content until it expires. A
presigned S3 POST cannot be made one-use without proxying the bytes; this bounded
replay window is an explicit residual risk. Quotas account for all versions and
requests, not merely current bytes. Deployments unable to bound that risk through
the short maximum TTL, provider account limits, monitoring, and principal-level
issuance limits must keep direct upload disabled.

#### Complete

The completion request has no body. A successful first completion returns
`201 Created`; a successful retry returns `200 OK`. Both return:

```json
{
  "id": "file_01...",
  "filename": "diagram.png",
  "mime_type": "image/png",
  "size": 483102,
  "uri": "threadmark://files/file_01...",
  "created_at": "2026-08-16T02:31:00Z"
}
```

Threadmark performs these steps:

1. Select the owner-scoped session. If it is completed, return its file. If it
   is expired or marked for cleanup, return `410` with
   `error.code = "upload_expired"`.
2. Atomically acquire a completion lease by changing `pending` to `finalizing`,
   or reclaiming `finalizing` only when its lease has expired. Reclamation marks
   the previous `copying` attempt `abandoned` in the same transaction. Another
   active finalizer returns `409` with
   `error.code = "upload_finalizing"` and a `Retry-After` header.
3. Issue `HEAD Object` for the staging key and record its S3 `version_id`. Bucket
   versioning must be `Enabled`; an absent, empty, or literal `"null"` version ID
   is an object-store configuration error that disables new direct initiations.
   A missing object returns `409` with
   `error.code = "upload_incomplete"` and restores the session to `pending` if
   it has not expired. Other S3 failures return `502` while leaving the lease
   reclaimable.
4. Verify the observed content length equals the declared `size`, does not
   exceed the current configured maximum, the stored content type equals the
   normalized MIME type, and the stored `threadmark-upload` metadata equals the
   session ID. Validation failure returns `409` with a specific stable code such
   as `upload_size_mismatch` or `upload_metadata_mismatch`, and restores the
   session to `pending` if unexpired so a still-valid URL may replace the staged
   object.
5. In a short transaction, verify the lease remains current and create one
   durable `copying` attempt containing the source version and a new random
   candidate key. There is at most one `copying` attempt per upload.
6. Copy the recorded immutable staging version to the attempt's unique candidate
   key. Disable AWS SDK retries for `CopyObject`: exactly one HTTP mutation may
   target a candidate key. Use metadata-directive `REPLACE`
   and tagging-directive `REPLACE`, setting only the normalized final content
   type, server-controlled metadata including the attempt ID, and
   `threadmark-lifecycle=uncommitted`. The destination has never been presigned
   for writing. A later completion attempt uses a different candidate key, so a
   stale request cannot overwrite its candidate. A timeout or any indeterminate
   result permanently abandons this attempt after its lease expires; the request
   is never retried against the same key. A delayed accepted request can therefore
   create only an uncommitted orphan, never a candidate later exposed as a file.
7. After an acknowledged copy, issue `HEAD Object` for the candidate key, require
   and record a nonempty, non-`"null"` candidate `version_id`, and verify its
   length and final metadata.
   This prevents a successful database commit on a partial or incompatible
   object-store response.
8. Change the candidate tag from `threadmark-lifecycle=uncommitted` to
   `threadmark-lifecycle=committed`, then read it back. A failure leaves no
   visible file and reconciliation restores the uncommitted tag for any candidate
   not selected by a completed PostgreSQL session. Disable SDK retries for the
   tag mutation. If its outcome is indeterminate, permanently abandon the attempt
   and never commit it. Reconciliation deletes the exact recorded candidate
   version regardless of its current tag; a delayed tag mutation cannot recreate
   a deleted version. Immediately before the tag change and again in step 9,
   verify the attempt is still `copying`, owns the session's current live lease,
   and has not been claimed by reconciliation.
9. In one PostgreSQL transaction, lock the session, verify that this worker still
   owns its lease, insert the `files` row using the preallocated file ID and this
   attempt's candidate key, mark the attempt committed, mark the session
   `completed`, and enqueue eventual deletion of the staging key. A uniqueness
   constraint on `file_uploads.file_id` and the locked session make exactly one
   candidate visible. The final file becomes visible only at this commit.
10. Attempt the staging-key deletion after commit. Failure remains in the durable
   deletion outbox and does not fail completion.

The completion lease is an internal random token plus `lease_expires_at`, not an
API credential. S3 operations do not occur while a PostgreSQL transaction or
row lock is held. `S3_OPERATION_TIMEOUT_SECONDS` bounds the AWS SDK's complete
operation, including retries; its default is 60 seconds. The default completion
lease is 120 seconds, must exceed that timeout, and is renewed between operations.
A worker whose lease is lost must not commit. Because provider acceptance after
a client-side timeout is still indeterminate, the combination of one mutation
per candidate, permanent abandonment after indeterminate results, and unique
candidate keys provides write fencing.

If Threadmark crashes after copying but before committing, no `files` row is
visible. After the attempt lease expires, a retry uses another candidate and can
commit without being overwritten by the stale copy. Reconciliation schedules
every abandoned candidate for deletion. If the session expires first, cleanup
claims it and schedules the staging key and all candidate keys. Cleanup and
completion use mutually exclusive conditional updates, so cleanup cannot claim
a session whose finalizer can still commit.

### Data model

Keep `files` unchanged. Add:

```sql
CREATE TABLE file_uploads (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_version smallint NOT NULL CHECK (request_version > 0),
    request_hash text NOT NULL,
    file_id text NOT NULL UNIQUE,
    filename text NOT NULL,
    mime_type text NOT NULL,
    expected_size bigint NOT NULL CHECK (expected_size >= 0),
    staging_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (
        status IN ('pending', 'finalizing', 'completed', 'cleanup_pending')
    ),
    lease_token text,
    lease_expires_at timestamptz,
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    CHECK ((status = 'completed') = (completed_at IS NOT NULL))
);

CREATE TABLE file_upload_attempts (
    id text PRIMARY KEY,
    upload_id text NOT NULL REFERENCES file_uploads(id) ON DELETE CASCADE,
    candidate_key text NOT NULL UNIQUE,
    source_version_id text NOT NULL,
    candidate_version_id text,
    status text NOT NULL CHECK (
        status IN ('copying', 'committed', 'abandoned', 'reconciling')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    settled_at timestamptz,
    UNIQUE (upload_id, id)
);
```

`file_id` is a reserved identifier, not a foreign key: pending sessions must
exist before the file, and retained completed sessions must not prevent later
file deletion. Completion verifies or inserts that exact owner-scoped file ID;
completed retries look it up and report `upload_file_deleted` when absent.
Production SQL will add checks that lease fields are present only for
`finalizing`, a partial unique index allowing at most one `copying` attempt and
one `committed` attempt per upload, and indexes for owner lookups, attempts, and
expiration cleanup.

Add a new `object_deletion_outbox` keyed by generated ID, with `storage_key`, a
required `version_id` for version-specific work or an explicit `all_versions`
work kind, `not_before`, and retry metadata. Version work is unique on
`(storage_key, version_id)`; at most one `all_versions` row exists per key. The
all-versions worker lists versions repeatedly after the settling window and
completes only after deleting every returned version and delete marker and a
subsequent listing is empty. Do not alter the existing
`file_deletion_outbox` in this rollout: old binaries continue processing it,
while new binaries process both tables. Existing file deletion can migrate to
the new table only in a later expand/contract release after old binaries are
drained. Upload completion enqueues the staging key with `not_before` after the
upload form expires plus the provider settling window. Expiration cleanup
enqueues staging and candidate keys. Deleting a missing object is success only
after `not_before`; a row is removed after confirmed deletion, while provider
lifecycle remains a second convergence mechanism for late accepted operations.

Keys are generated, never derived from caller text:

```text
uploads/{tenant_component}/{owner_component}/{upload_id}/{random_nonce}
upload-candidates/{tenant_component}/{owner_component}/{attempt_id}/{random_nonce}
```

Tenant and owner components use a collision-free encoding of the complete
verified identifier, not raw path text or a lossy sanitizer. IDs and the nonce
provide unguessability; authorization does not depend on key secrecy.

### Object-store requirements

Extend `ObjectStore` with:

- presigned POST policy against the public-endpoint client, including exact
  content-length range and exact-match fields;
- version-aware `HEAD Object`, including content length, content type, user
  metadata, and `version_id`;
- server-side `CopyObject` of an explicit source version using the internal
  client, with replaced metadata and tags and operation-specific retries
  disabled;
- object tag update and inspection using the internal client;
- version-specific object deletion and version listing for reconciliation;
- error classification for not-found versus transient failures.

Startup continues to use the internal endpoint for bucket access. When direct
upload is enabled, startup also validates configuration needed to construct a
public presigner. A deployment smoke test, rather than a destructive startup
probe, verifies browser-reachable DNS/TLS, CORS, signed POST, version-aware HEAD,
copy, tagging, versioned delete, and lifecycle against the actual provider.
Required object actions include `s3:PutObject` on staging and candidate keys;
`s3:GetObject`, `s3:GetObjectVersion`, `s3:GetObjectTagging`, and
`s3:GetObjectVersionTagging` as needed for HEAD, copy, and verification;
`s3:PutObjectTagging` and `s3:PutObjectVersionTagging`; and `s3:DeleteObject` and
`s3:DeleteObjectVersion` on both prefixes. Required bucket actions include
`s3:ListBucketVersions`, constrained to the managed prefixes, plus read access to
versioning, encryption, and lifecycle configuration used by startup validation.
(`HeadObject` is an API operation authorized by the applicable get-object action,
not an IAM action.) SSE-KMS additionally requires the provider-documented KMS
actions. Browser authority is restricted by the signed request to one staging
key and short expiration.

Bucket versioning and bucket-default encryption are prerequisites and must apply equally to staging
and candidate prefixes; this version does not support bucket policies requiring
explicit browser SSE headers. Copy uses configured SSE-S3 or SSE-KMS settings,
including key ID and required IAM/KMS permissions. Lifecycle must expire every
staging version, all uncommitted candidate versions, every noncurrent candidate
version including formerly committed files, and expired delete markers. Thus the
existing unversioned finalized-file delete creates a delete marker immediately,
while lifecycle later reclaims the now-noncurrent committed bytes. Quota and
monitoring include noncurrent versions.

Enabling versioning affects legacy objects written by `POST /v1/files` under the
existing owner-prefixed keys. Before versioning is enabled, lifecycle rules must
also expire noncurrent versions and delete markers under that legacy prefix, or
legacy deletion must first migrate to the version-aware outbox. This prerequisite
prevents the existing unversioned delete and failed-database-insert cleanup paths
from leaking billable versions.

The object-store credential provider uses the AWS default credential chain and
supports refreshable role credentials, including session tokens. Presigned POST
fields and policy conditions include `x-amz-security-token` when present. Form
expiration is bounded by the earliest of configured URL TTL, session expiration,
and credential expiration minus clock-skew margin; initiation fails closed if
credentials cannot remain valid for a configured minimum form lifetime. Static
credentials remain supported for local MinIO.

### Visibility and references

No pending row exists in `files`, so current metadata reads, content reads,
download grants, replay hydration, item-reference checks, and turn snapshots
cannot observe an upload session. A canonical URI is returned only after
completion commits. Existing `FOR KEY SHARE` reference locking and restrictive
foreign keys continue to serialize references against finalized-file deletion.

### Expiration and cleanup

Every minute and once at startup, a bounded cleanup pass uses `FOR UPDATE SKIP
LOCKED` to claim batches of:

- `pending` sessions whose `expires_at` has passed; and
- `finalizing` sessions whose upload has expired and whose completion lease has
  also expired.

Claiming changes the status to `cleanup_pending`, clears lease fields, marks any
uncommitted attempts abandoned, and enqueues the staging and all candidate keys
in one database transaction. Completion cannot acquire a lease from
`cleanup_pending`. Cleanup never enqueues the candidate referenced by a completed
file. After outbox entries are removed, the upload-session row may be retained
for a configured idempotency retention period and then deleted. Completed
session rows must be retained at least as long as clients are expected to retry
their idempotency keys; deleting one does not delete its file.

Cleanup is horizontally safe: conditional state changes, `SKIP LOCKED`, and the
outbox's unique storage key prevent duplicate work. Passes use bounded batches
so a large backlog does not delay server startup or monopolize the database.

Provider lifecycle is required, not optional. Staging objects use a dedicated
prefix whose rules expire current objects, all noncurrent versions, and delete
markers after a period longer than the maximum upload session, form lifetime,
operation timeout, and provider settling window.
Every candidate copy is created with `threadmark-lifecycle=uncommitted`; a
provider rule expires that tag. Before the database transaction in completion,
Threadmark changes the verified candidate's tag to
`threadmark-lifecycle=committed` and verifies it, making that candidate exempt.
A crash after the tag change but before commit is handled by reconciliation.
Upload and attempt tombstones are retained through the lifecycle window. The
reconciler first conditionally changes only an `abandoned` attempt whose session
has no live lease to `reconciling`; finalizers can operate only on their current
`copying` attempt, making these claims mutually exclusive. It then HEADs overdue
keys not committed in PostgreSQL, restores the uncommitted tag where useful, and
enqueues deletion of the exact `candidate_version_id`. If an indeterminate copy
did not return a version ID, it lists that unique candidate key's versions and
enqueues all of them. Version deletion is unconditional with respect to tags.
The deletion worker rechecks PostgreSQL immediately before deleting a candidate
and cancels the outbox row if the attempt became committed. Tombstones and
reconciliation remain active until version deletion is confirmed after the
provider settling window. The provider rule
ultimately removes a delayed PUT or copy even if it appears after an earlier
delete. Deployment validation fails if tag-filtered lifecycle and versioning
rules are absent. Lifecycle periods must leave enough margin for a live
finalizer to commit the tag change before provider expiration evaluation.

### Existing multipart endpoint

`POST /v1/files` remains behaviorally compatible for non-browser clients during
alpha. Documentation labels it server-mediated and memory-buffered. It continues
to produce only finalized `files` rows. Follow-up work may stream its request
body or remove it after clients migrate; neither is required for this RFC.

### Errors and observability

Expected client state errors use stable codes described above. Database and
unexpected S3 failures use the existing non-disclosing error envelope. Logs and
metrics include upload status transitions, initiation/completion counts and
latency, validation failures by reason, lease reclaims, expired sessions,
outbox depth/oldest age, cleanup attempts, and staged/final byte counts. Logs
include upload and file IDs but never presigned URLs, authorization headers,
signatures, S3 credentials, or raw object keys containing owner identifiers.

Rate limits and quotas are enforced by the API edge per authenticated principal
and client for initiation and completion. Exact-size POST policy conditions,
provider request-rate/cost controls, required lifecycle rules, and alarms on
direct upload volume cover traffic that bypasses the API after form issuance.

Direct upload is gated by `DIRECT_UPLOAD_ENABLED`, default `false`. Startup fails
when it is true unless `S3_PUBLIC_URL`, versioning, encryption, lifecycle, TTL,
timeout, and lease settings pass read-only validation and a deployment-provided
`DIRECT_UPLOAD_CAPABILITY_ATTESTATION` matches the current bucket, endpoint, and
configuration fingerprint. The attestation is emitted only by the destructive
deployment smoke test after POST, copy, versioned HEAD/tag/delete, version
listing, CORS, and lifecycle checks succeed with the runtime role. Startup does
not claim that read-only IAM inspection proves mutation permissions. This permits
additive code and schema rollout, infrastructure validation, and old-binary
drainage before the endpoint is enabled. Disabled initiation returns `503` with
`direct_upload_unavailable`; existing direct downloads remain independent.
Threadmark periodically rechecks versioning and the other read-only bucket
invariants while enabled. Drift stops new initiation immediately; completion
also requires non-`"null"` source and candidate versions and therefore fails
closed if versioning was suspended between checks.

## State Machine

```text
                  acquire lease
pending ------------------------------> finalizing
   |                                      |   |
   | validation failure, unexpired <------+   | successful DB commit
   |                                          v
   |                                      completed
   |
   | expired                         expired + stale lease
   +---------------------> cleanup_pending <---+
```

There is no transition out of `completed` or `cleanup_pending`. File deletion is
the existing, separate finalized-file lifecycle.

## Security Properties

- Threadmark derives owner scope only from the verified access token.
- A browser receives write authority for one random staging key, never a final
  key, prefix, bucket listing operation, read operation, or delete operation.
- Signed content type, exact-size policy, and upload-ID metadata prevent changing
  those values without invalidating the form; completion independently verifies
  them.
- Declared and observed sizes are checked before copy and before database commit.
- Short URL and session lifetimes bound credential theft and abandoned storage.
- Per-attempt server-side copy destinations prevent stale copies or browser form
  replays from mutating the candidate selected by the committed file row.
- Pending sessions never satisfy the `files` foreign key used by transcripts.
- Owner predicates and opaque `404` responses prevent cross-owner discovery.
- Presigned URLs are bearer credentials and are excluded from logs and durable
  client telemetry.

This design does not prove that bytes match their filename or MIME declaration.
Consumers must continue treating served content as untrusted; downloads retain
`Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`.

## Rollout

1. Add the schema, generalized object-deletion outbox, object-store operations,
   configuration, API, and unit/integration tests behind direct-upload support.
2. Configure staging/final bucket permissions, CORS, versioning, encryption, and the
   required staging/candidate lifecycle and versioning rules in a non-production
   environment. Deploy the additive outbox schema without changing the old one.
3. Run provider and browser smoke tests, including MinIO and the production S3
   provider. Verify no payload bytes transit Parley or Threadmark. Leave
   `DIRECT_UPLOAD_ENABLED=false`.
4. Drain old binaries, update Parley to initiate, POST, and complete uploads with
   retries using one
   persisted idempotency key. Keep the old endpoint as fallback only for clients
   that cannot directly reach S3.
5. Enable metrics and alerts for cleanup backlog and object-store errors, then
   set `DIRECT_UPLOAD_ENABLED=true`.

## Testing

- Introduce injectable object-store and clock interfaces, deterministic lease and
  ID sources in tests, and single-pass cleanup/reconciliation functions.
- Unit-test normalization, request hashing, state transitions, lease expiry,
  error mapping, key encoding, and presigned policy construction.
- Integration-test exact and mismatched initiation retries, concurrent
  initiation, no object, wrong size/type/metadata, zero-byte files, over-limit
  files, expired sessions, and cross-owner access.
- Race completion against completion, expiration cleanup, and file references;
  assert exactly one final file and no deletion of committed content.
- Inject crashes/failures before and after staging HEAD, copy, final HEAD, file
  insertion, session completion, outbox insertion, and S3 deletion. Retrying or
  cleanup must converge without a visible partial file or permanent orphan.
- Simulate an indeterminate copy that is accepted after client timeout; assert no
  second request targets that candidate, lease reclamation abandons it, and only
  a different candidate can be committed.
- Simulate an indeterminate committed-tag update accepted after timeout; assert
  the attempt cannot commit and reconciliation deletes its exact object version
  regardless of the delayed tag value.
- Verify a POST replay after completion changes only the staging key and cannot
  change downloaded finalized bytes.
- Verify multipart upload remains compatible.
- Run browser CORS tests and payload-integrity checks against MinIO and the
  production-compatible provider using files at zero, typical, and maximum size.
- In a versioning-enabled provider test, replay an upload form, abandon copies,
  complete and then delete a file, and verify lifecycle eventually removes all
  staging versions, noncurrent candidate versions, and delete markers without
  deleting any live committed file.
- Suspend versioning after startup and assert both `"null"` staging/candidate
  versions and read-only configuration drift disable initiation/finalization
  before a file can be committed.
- With versioning enabled, exercise legacy multipart upload, normal deletion, and
  database failure after PUT; verify lifecycle or version-aware cleanup reclaims
  all versions and delete markers.
- Test refreshable temporary credentials, required session-token POST fields,
  credential rotation, and form expiration bounded by credential expiration.
- Load-test concurrent maximum-size uploads and assert Threadmark and Parley
  memory do not scale with payload bytes.
- Verify logs, traces, and error reports redact presigned URLs and signatures.

## Drawbacks

- Upload is now a three-request workflow and requires browser access to the
  object-store endpoint.
- Server-side copy temporarily stores multiple objects during failed attempts
  and adds S3 requests and copy latency. For the current 32 MiB limit this is
  preferred to mutable final keys.
- PostgreSQL/S3 coordination requires leases, an outbox, and reconciliation.
- S3-compatible providers differ in CORS, metadata, copy, and error behavior, so
  provider-level tests are mandatory.
- A valid upload form is replayable with correctly sized content until expiry,
  so provider-side request-rate and cost controls remain necessary.

## Alternatives

### Presign the final key

Rejected because a successful completion could be followed by another valid
`PUT` to the same key before URL expiry, mutating a referenced file.

### Add pending columns directly to `files`

Rejected because existing code assumes every `files` row is readable and
referenceable. A separate upload resource keeps that invariant structural and
reduces the chance that one query forgets a `status = 'completed'` predicate.

### Stream Browser -> Parley -> Threadmark -> S3

This would reduce buffering if both proxies stream correctly, but payload bytes
would still consume bandwidth, connections, and failure domains in both
services. It is useful only as a fallback for clients unable to reach S3.

### Presigned PUT

Rejected because a standard browser presigned PUT cannot portably enforce the
declared object length at the provider. Checking only at completion permits one
valid URL to drive oversized request and storage costs directly against S3.

### Require a client SHA-256 digest

A signed checksum and completion-time verification would strengthen integrity,
but Web Crypto's common `digest()` API is not streaming and can add a full
browser-memory copy. S3-compatible checksum reporting also varies. The first
version relies on TLS, SigV4 payload transport, size/metadata verification, and
final copy. A later optional streaming checksum can be added without changing
the state machine.

### Multipart or resumable S3 upload

Not justified by the current maximum size. It adds part signing, completion,
abort, and stale multipart cleanup. It should be a separate RFC if file limits
grow substantially.

## Unresolved questions

None. The exact SQL constraint syntax, lease duration, cleanup batch size, and
provider IAM/CORS documents are implementation details selected within the
invariants and defaults above.
