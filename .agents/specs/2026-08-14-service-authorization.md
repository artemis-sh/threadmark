---
title: "Threadmark Service Authorization"
status: final
kind: rfc
created: 2026-08-14T05:30:24+00:00
---

# RFC: Threadmark Service Authorization

## Summary

Threadmark will replace caller-supplied identity headers with short-lived,
asymmetrically signed bearer access tokens. Threadmark will authenticate each
token against a configured issuer and derive the tenant, principal, caller,
permissions, and optional resource bounds exclusively from verified claims.

The design supports two token profiles:

- **Owner-session tokens** let the Parley backend act for one tenant and
  principal across that principal's resources.
- **Delegated-agent tokens** grant a named agent only the operations and
  conversation or turn needed for one execution.

Token issuance and user authentication remain outside Threadmark. Threadmark is
the resource server: it verifies credentials, applies authorization policy, and
continues enforcing tenant/owner predicates in database queries.

## Motivation

Threadmark currently constructs `Actor` directly from
`X-Threadmark-Tenant` and `X-Threadmark-Principal`. Those headers are arbitrary
caller input. Any process that can reach the API can impersonate another owner,
read private continuation state, alter a transcript, or delete data.

Authenticating only the Parley service while continuing to accept arbitrary
identity headers would establish who called Threadmark but would not constrain
whom that caller may impersonate. Authorization claims must therefore be signed,
short-lived, and checked against the requested operation and resource.

## Goals

- Authenticate every `/v1` request except a deliberately minimal health check
  and redemption of an independently signed download capability.
- Derive tenant and principal only from verified claims.
- Give Parley the owner-wide operations its server adapter requires.
- Give agents least-privilege, resource-bound capabilities.
- Keep existing tenant and owner SQL predicates as defense in depth.
- Support key rotation, incident response, and useful audit records.
- Fail closed on malformed credentials, unknown permissions, and configuration
  mistakes.

## Non-goals

- User login, sessions, or deciding whether a Parley user belongs to a tenant.
- A general-purpose RBAC administration system inside Threadmark.
- Sharing conversations between principals.
- Immediate proof-of-possession tokens. Bearer tokens are the first production
  profile; mTLS-bound tokens can be added where the infrastructure supports it.
- Replacing the existing signed file-download capability. Its key separation
  and claim format will be improved, but it remains a separate credential type.

## Trust Model

An authorization issuer authenticates Parley and is trusted to issue Threadmark
claims. It may be Parley's authorization service or an independent internal
issuer, but its signing key is not held by Threadmark. Threadmark receives only
public verification keys.

Parley requests an owner-session token after authenticating its user and
authorizing tenant membership. When invoking an agent, Parley or the issuer
exchanges that authority for a narrower delegated-agent token. An agent never
receives an owner-session token.

Compromise implications are explicit:

- A stolen owner-session token is limited to one tenant/principal and its short
  lifetime.
- A stolen delegated token is limited to its permissions and resource bounds.
- A compromised Parley backend can request authority available to its issuer
  identity. Issuer policy should constrain allowed tenants and delegation; no
  token format can compensate for an unconstrained or compromised issuer.
- A stolen signing key compromises all authority accepted for that issuer. Key
  isolation, rotation, and emergency disablement are required.

TLS is mandatory outside local development. Private networking remains useful
defense in depth, not the authentication mechanism.

## Credential Profile

### Format and verification

Access tokens are compact JWTs signed with Ed25519 (`alg=EdDSA`). Threadmark
accepts only that configured algorithm, never `none` or an algorithm selected
from untrusted token input. Each JWT header contains `typ=at+jwt` and a `kid`.

Threadmark loads keys from an HTTPS JWKS URI for the configured issuer, caches
them for at most the response cache lifetime with a configured upper bound, and
refreshes once when an otherwise valid token has an unknown `kid`. Unknown-key
refreshes are single-flight and subject to a minimum refresh interval and a
short, bounded negative cache, so unique attacker-supplied key IDs cannot cause
one outbound request each. Known cached keys remain usable during that interval.
The last valid key set may be used only for a configured stale-if-error interval;
after that Threadmark fails closed. Duplicate `kid` values, incompatible key
types, redirects outside the configured issuer origin, oversized responses, and
an empty usable key set are rejected. Production startup fails if issuer,
audience, or JWKS configuration is absent.

Verification requires:

- a valid Ed25519 signature;
- exact `iss` match against `AUTH_ISSUER`;
- `aud` encoded as either the configured audience string or a singleton array
  containing it; multi-audience tokens are rejected;
- integer NumericDate `exp`, `iat`, and `nbf` claims with at most 30 seconds of
  clock skew;
- `exp > iat`, `exp > nbf`, `iat <= now + skew`, `nbf <= now + skew`, and
  `now < exp + skew`;
- `exp - iat` no greater than the profile maximum;
- nonempty `sub`, `client_id`, `jti`, `tenant`, and `principal` strings within
  documented length limits;
- a recognized `token_kind` and only recognized permission strings;
- syntactically valid optional resource-bound claims.

The raw token is accepted only in `Authorization: Bearer <token>`. Cookies,
query parameters, and identity headers are not authentication inputs. Tokens
and signed download URLs must be redacted from application, proxy, and tracing
logs.

### Claims

```json
{
  "iss": "https://auth.internal.example",
  "aud": "threadmark-api",
  "sub": "svc_parley",
  "client_id": "parley-prod",
  "iat": 1786680000,
  "nbf": 1786680000,
  "exp": 1786680300,
  "jti": "01K...",
  "token_kind": "owner_session",
  "tenant": "acme",
  "principal": "user_123",
  "permissions": ["conversation:list", "conversation:create", "conversation:read"]
}
```

`sub` identifies the authenticated service or agent deployment. `client_id` is
the registered OAuth-style client identity used for policy and audit. Neither
is the end user. `tenant` and `principal` form Threadmark's `Actor`.

Delegated tokens add resource bounds:

```json
{
  "token_kind": "delegated_agent",
  "tenant": "acme",
  "principal": "user_123",
  "conversation_id": "conv_...",
  "turn_id": "turn_...",
  "agent_ref": "research-agent/prod",
  "permissions": ["transcript:read", "transcript:append_agent", "turn:read", "turn:update", "continuation:write"]
}
```

For `delegated_agent`, `conversation_id` and `agent_ref` are required. `turn_id`
is required whenever a permission can mutate an existing turn or append agent
output. Delegated tokens may live for at most 10 minutes. Owner-session tokens
may live for at most 5 minutes. The issuer may choose shorter lifetimes.

Permissions do not imply one another. Unknown permissions invalidate the token
rather than being ignored, preventing a rollout typo from silently weakening
issuer expectations.

Token kind also constrains permissions. A `delegated_agent` token is invalid if
it contains anything outside `transcript:read`, `transcript:append_agent`,
`turn:read`, `turn:update`, `continuation:read`, `continuation:write`, and
replay-scoped `file:read`. In particular, delegated tokens can never carry
`transcript:append`, `turn:create`, conversation administration, or direct file
permissions. Permission recognition alone does not make a permission valid for
the token kind.

## Authorization Model

After authentication, middleware creates an immutable `AuthContext` containing
the actor, service identity, token ID, kind, permissions, and resource bounds.
Handlers declare a required permission set and any request-dependent additional
permissions. A shared authorizer requires all of them and checks route/body
constraints before the operation proceeds.

Resource authorization is the intersection of all constraints:

1. Every permission required by the route and selected response mode must be
   present.
2. A `conversation_id` claim must equal the route conversation or the parent
   conversation resolved for a turn or continuation.
3. A `turn_id` claim is the target mutation bound. It must equal a turn supplied
   or resolved for append, turn update, and continuation write, but it does not
   limit historical transcript items read from the bound conversation.
4. An `agent_ref` claim must equal every supplied or resolved agent reference.
5. The resource must match the signed tenant and principal through existing SQL
   ownership predicates.

The resource lookup and mutation must not create a time-of-check/time-of-use
gap. Mutating store methods enforce ownership and token bounds in the mutation
query or in the same transaction while locking the relevant conversation.

Delegated agents have these additional invariants:

- They cannot create, list, update, delete, truncate, or regenerate
  conversations.
- `transcript:append_agent` requires `source=agent`, the bound `turn_id`, and a
  turn belonging to the bound conversation with the bound `agent_ref`. Because
  replay discards the outer `source` and protocol payloads are otherwise opaque,
  delegated appends additionally pass a strict allowlist validator for agent
  output item types and roles. User/system roles, input item forms, unknown item
  types, and unknown role-bearing payloads are rejected. The validator is
  versioned with the supported Open Responses contract; delegated writes for a
  newly supported item type remain disabled until its safe shape is classified.
  An exact authorized idempotency replay is resolved first. A new batch is
  accepted only while the bound turn is `pending` or `streaming`, checked while
  the append transaction holds the conversation/turn lock; terminal turns accept
  no new output.
- They cannot introduce a `threadmark://files/...` reference. Delegation carries
  an `allowed_file_ids` claim containing the exact owner files the issuer made
  available for the run, and replay may hydrate only those IDs or IDs already
  present in the transcript before the bound turn was created. The pre-turn set
  is captured transactionally when the turn/delegation is created, not inferred
  from a later mutable transcript.
- They cannot upload or delete owner files or mint general download grants.
- They can receive file content only through short-lived capabilities generated
  while replaying their bound conversation.
- Continuation reads and writes require the bound `agent_ref`. A delegated token
  may read only response IDs listed in its signed `readable_continuation_ids`
  claim; this permits an explicitly delegated previous-turn checkpoint without
  exposing every private checkpoint for that agent and conversation. Writes
  require the bound conversation and current turn. The continuation schema/API
  will carry `owner_ref` and `turn_id` before agent access is enabled. Uniqueness is
  `(tenant_id, owner_ref, agent_ref, response_id)`. A parent continuation is
  resolved in the insertion transaction and must have the same actor,
  conversation, and agent. Existing rows receive a `turn_id` only when an
  unambiguous match exists. Delegated writes reject a null or mismatched current
  turn. A delegated read may access a legacy null-turn row only when its response
  ID is explicitly listed in `readable_continuation_ids`; owner-session access
  may continue.
- Continuation creation is idempotent by its uniqueness key. An exact retry must
  match actor, conversation, turn, agent, response ID, parent, transcript
  position, and state and returns the existing row. Any differing retry returns
  `409` without disclosing the existing state.
- Append idempotency records store a canonical request digest, `source`, and
  `turn_id`. An existing idempotency key returns its batch only when those values
  and all authorization-relevant bounds match; otherwise the request returns
  `409`. This check occurs before returning any prior payload.
- Turn state transitions are monotonic (`pending` to `streaming` or a terminal
  state, and `streaming` to a terminal state). An exact repeat of any previously
  applied update is idempotent, including `pending` and `streaming`; same-status
  requests that alter protected outcome fields, regressions, and conflicting
  terminal rewrites fail.

### Permission mapping

| Permission | Operations |
| --- | --- |
| `conversation:list` | `GET /v1/conversations` |
| `conversation:create` | `POST /v1/conversations` |
| `conversation:read` | Read one conversation's metadata |
| `conversation:update` | `PATCH /v1/conversations/{id}` |
| `conversation:delete` | `DELETE /v1/conversations/{id}` |
| `conversation:truncate` | `POST .../{id}/truncate` |
| `conversation:regenerate` | `POST .../{id}/regenerate` |
| `transcript:read` | List items and create replay projections |
| `transcript:append` | Append owner-authorized `user`, `agent`, or `system` items |
| `transcript:append_agent` | Append only bound agent output under the delegated invariants |
| `turn:create` | Create a turn; any `agent_ref` claim must match |
| `turn:read` | List/read turns, subject to resource bounds |
| `turn:update` | Update a turn, subject to turn and agent bounds |
| `continuation:read` | Resolve continuation state, subject to agent/resource bounds |
| `continuation:write` | Create continuation state, subject to agent/resource bounds |
| `file:create` | Upload an owner file |
| `file:read` | Read owned file metadata or authenticated content |
| `file:delete` | Delete an unreferenced owned file |
| `file:grant` | Mint a signed file-download capability |

`GET /v1/conversations/{id}/turns` requires `turn:read`, not merely
`conversation:read`. `GET .../active-turn` requires both `conversation:read`
and `turn:read`. Replay requires `transcript:read`; requesting a delivery mode
that emits file bytes or URLs additionally requires `file:read`. These compound
requirements prevent data from crossing a permission boundary through a
projection endpoint.

For a token with `turn_id`, direct `GET /v1/turns/{id}` may return only that
turn. Turn listing and active-turn summary either return only the bound turn or
`404`; they never disclose other turns. `transcript:read` deliberately reads the
history of the entire bound conversation because an agent needs prior context;
the turn bound constrains mutations, not historical transcript visibility.

Owner-session tokens may carry any owner permission approved for the Parley
client. The issuer should issue a task-specific permission set rather than a
single permanent role encoded in Threadmark.

## Download Capabilities

`GET /v1/downloads/files/{id}` remains unauthenticated by access token because
the signed URL is itself a bearer capability. Minting it requires `file:grant`
or an authorized replay operation. Delegated replay may mint a capability only
for its captured/claimed allowed-file set; write access cannot expand that set.
Files remain owner-wide records with no parent-conversation relationship; the
bound conversation and its immutable allowed-file set are the exclusive basis
for delegated hydration. Delegated callers cannot use direct file endpoints.

The capability is changed to a versioned token containing file ID, tenant,
principal, delivery mode, `iat`, `exp`, a random `jti`, and optional
conversation/turn audience. It is signed with a dedicated download-capability
key, not an access-token issuer key and not a generic application secret. TTL is
at most 5 minutes for replay and 15 minutes for an explicit owner download.

Capabilities provide confidentiality only while their URLs remain secret. They
must not appear in access logs or referrer headers. Responses use
`Referrer-Policy: no-referrer`, `Cache-Control: private, no-store`, and the
existing content-disposition and content-type protections. Revocation before
expiry is not guaranteed; deletion and ownership checks still occur at
redemption.

For redirect delivery, the generated S3 URL expires no later than the presented
capability. Its TTL is `min(capability_exp - now, configured S3 maximum)`, so
redemption near expiry cannot extend effective access beyond the signed grant.
Every direct presigned S3 URL emitted by delegated replay is likewise limited to
five minutes and to no later than the delegated access token's remaining
lifetime. This applies independently of the general capability TTL setting.

## Replay and Revocation

Access tokens are short-lived bearer credentials. TLS, narrow permissions,
resource binding, and redaction are the primary replay controls. `jti` is always
logged in hashed form and can be placed on a denylist until `exp` for emergency
revocation. Threadmark consumes a signed, bounded issuer/client/token revocation
snapshot at least every 30 seconds. A snapshot may be no more than 60 seconds
old; after that all protected production operations fail closed. Client or token
disablement therefore propagates within 60 seconds when the control plane is
healthy. This availability cost is intentional and is covered by alerts and an
operator runbook. A deployment may instead use synchronous introspection if it
provides equal or stronger freshness and fail-closed behavior.

Threadmark does not reject every repeated `jti`: retries are required. Every
mutation exposed to delegated agents must be idempotent and integrity-preserving
before access is enabled. High-risk owner deletions and transcript
truncation/regeneration are not granted to delegated tokens.

Disabling an issuer/client or removing a signing key must be operable without a
deployment. Emergency key refresh and client denylisting are required runbook
operations.

## Errors and Disclosure

- Missing, malformed, expired, or unverifiable credentials return `401` with a
  standards-compatible `WWW-Authenticate: Bearer` response and no verification
  details.
- Valid credentials lacking an operation permission return `403`.
- A valid token attempting to access a resource outside its actor or resource
  bounds returns `404`, preserving the current non-enumeration behavior.
- Authorization failures are structured audit events but never include token
  contents, private claims beyond stable IDs, or resource payloads.

The public health endpoint reports only process liveness. Dependency readiness
details move to an authenticated or network-restricted endpoint so unauthenticated
callers cannot use it for infrastructure discovery or denial-of-service load.

## Audit and Observability

For every authenticated request, record timestamp, request ID, hashed `jti`,
issuer, `sub`, `client_id`, token kind, tenant, principal, required permission,
resource IDs, decision, status, and latency. Do not record bearer tokens,
continuation state, transcript payloads, file URLs, or full query strings.

Audit records must be append-only from the application's perspective, retained
according to security policy, and searchable for a tenant, principal, client,
or token ID. Alert on signature failures, unknown keys, expired-token spikes,
cross-bound attempts, denylisted clients, and unusual destructive operations.

## Configuration

Production requires:

```text
AUTH_MODE=jwt
AUTH_ISSUER=https://auth.internal.example
AUTH_AUDIENCE=threadmark-api
AUTH_JWKS_URL=https://auth.internal.example/.well-known/jwks.json
AUTH_MAX_OWNER_TOKEN_SECONDS=300
AUTH_MAX_DELEGATED_TOKEN_SECONDS=600
DOWNLOAD_CAPABILITY_KEYS=<secret-manager reference or mounted key ring>
DOWNLOAD_CAPABILITY_ACTIVE_KID=...
```

Secrets come from a secret manager or read-only mounted secret, not checked-in
environment files. Clock synchronization is monitored because token validity
depends on it.

`AUTH_MODE=trusted_headers` exists only in debug builds or an explicitly built
development artifact, binds by default to loopback, emits a startup warning,
and cannot be selected in the production image. There is no runtime fallback
from JWT verification to trusted headers.

## Migration

1. Introduce `AuthContext`, permission declarations, authorization tests, JWT
   verification, and audit events while retaining trusted headers only in the
   development artifact.
2. Add nullable continuation `owner_ref` and `turn_id`. Backfill `owner_ref` from
   each referenced conversation, verify no nulls or actor mismatches remain, set
   it `NOT NULL`, then replace the unique constraint. Backfill only unambiguous
   turn matches, validate parent chains, and apply the explicit legacy-row read
   and write rules.
3. Extend append idempotency records with authorization-relevant request
   identity. Add those columns as nullable, backfill canonical digest, source,
   and turn only where the complete original batch can be proven, then make the
   fields non-null. Delete legacy records whose identity cannot be reconstructed,
   or permanently reject reuse of their keys; never return their prior payload.
   Add the delegated payload allowlist and capture each newly created turn's
   allowed input files. Existing turns are marked as lacking an authoritative
   pre-turn snapshot and cannot receive delegated access; timestamp-based
   reconstruction is forbidden. Explicit signed `allowed_file_ids` may still be
   used. Enforce monotonic turn transitions and serialize file deletion against
   reference creation by locking the file row in the append/deletion database
   transactions. In the same transaction that removes a file row, write a
   deletion outbox/tombstone containing its storage key. Remove the outbox row
   only after successful object deletion, allowing cleanup to survive failures
   and process restarts without leaving an authorized row pointing at missing
   content.
4. Configure the issuer and Parley token acquisition in a non-production
   environment. Run negative integration tests for actor, conversation, turn,
   agent, permission, expiry, issuer, audience, algorithm, and key boundaries.
5. Before rotating download capabilities, enforce a hard maximum on the legacy
   TTL. Deploy the versioned format and retain the legacy verifier/key only until
   that maximum has elapsed after the last old grant could be issued, then remove
   both. Emergency rotation may intentionally invalidate outstanding grants.
6. Deploy production with JWT mode mandatory, network policy allowing only
   expected clients, TLS, log redaction, and issuer/client disablement runbooks.
   Use an atomic blue/green cutover: remove every trusted-header replica from all
   reachable routing before exposing the JWT fleet. A rolling mixed-auth fleet
   is forbidden unless one gateway enforces JWT for every old and new replica.
7. Remove trusted-header code after local tooling uses development tokens or a
   dedicated local issuer.

The rollout must not support requests that combine a service credential with
unsigned tenant/principal headers. During migration, each deployment uses one
complete authentication mode.

## Verification Plan

- Unit-test claim parsing and every rejection rule, including algorithm
  confusion, duplicate/unknown `kid`, malformed claim types, excessive lifetime,
  skew boundaries, unknown permissions, and resource-bound claim combinations.
- Table-test every route against every permission and both token kinds.
- Integration-test cross-tenant, cross-principal, cross-conversation,
  cross-turn, and cross-agent attempts, including IDs supplied indirectly in
  bodies or resolved through parent resources.
- Test that append and continuation constraints remain true under concurrent
  changes, idempotency-key collisions, altered retries, parent references, and
  legacy rows.
- Test delegated payload-role confusion, unknown item types, file-ID injection,
  post-terminal append, turn-list filtering, exact nonterminal retries, status
  regression, and conflicting terminal updates.
- Test continuation exact/conflicting retries and prove a delegated token can
  read only its explicitly listed previous-response IDs, including legacy rows.
- Race file reference creation against deletion and verify object cleanup can be
  safely retried from the durable outbox after storage failures and restarts.
- Test key overlap rotation, emergency removal, JWKS outage/staleness, client
  denylisting, stale revocation state, unknown-`kid` flooding, single-flight
  refresh, legacy capability expiry, redirect and direct-presign lifetime
  capping against token expiry, and clock drift.
- Reject every owner-only permission in a delegated token, and test legacy turns
  and unprovable append batches fail closed after migration.
- Verify logs and traces never contain authorization headers, JWTs, download
  signatures, continuation state, transcript payloads, or presigned S3 URLs.
- Run end-to-end tests in which Parley obtains an owner token, creates a bounded
  delegated token, and proves the agent cannot escape its conversation, turn,
  agent identity, source, or permission set.

Production readiness requires all negative authorization tests to pass and an
operator exercise of signing-key rotation and client revocation.

## Drawbacks

- Running or integrating an issuer and JWKS endpoint adds operational work.
- Asymmetric verification, revocation checks, and audit delivery add latency and
  failure modes.
- Fine-grained permissions and resource-bound agents require explicit policy at
  every new route.
- Bearer credentials remain replayable until expiry unless deployment-specific
  proof of possession is later adopted.
- Adding `turn_id` to continuations changes the experimental API and schema.

## Alternatives

### Static API key plus identity headers

This authenticates Parley but leaves it free to assert any actor and gives every
holder permanent broad authority. It is acceptable only as a short-lived local
integration aid, not production authorization.

### HMAC-sign every request

Request signing can prevent tampering and replay, but shared secrets complicate
rotation, put verification secrets in Threadmark, and do not by themselves
define resource authorization. It is useful only if required infrastructure
cannot support asymmetric tokens.

### mTLS only

mTLS strongly authenticates workloads but a certificate normally identifies a
service, not the end-user actor, permission set, or conversation/turn boundary.
It is valuable in addition to claims and may later bind tokens to a certificate.

### Opaque tokens with introspection

Central introspection improves immediate revocation and avoids exposing claims
to clients, but makes every request depend on an authorization service unless
carefully cached. This is a valid alternative if immediate revocation is more
important than local verification; the authorization model in this RFC remains
the same.

### Threadmark token-minting endpoint

This would couple the ledger to user/service authentication and delegation
policy. Keeping issuance external makes the trust boundary and key ownership
clearer. Threadmark should mint only resource capabilities derived from already
authorized requests, such as file-download grants.

## Unresolved Questions

- Which component will act as issuer, and how will it constrain Parley's tenant
  and principal assertions?
- Will production require a shared revocation store or a periodically fetched
  signed denylist?
- Is mTLS available between Parley, agents, and Threadmark for a later
  proof-of-possession profile?
- What audit retention and tenant-access policies apply?

These deployment choices do not change the token claims, route policy, or
fail-closed requirements in this proposal.
