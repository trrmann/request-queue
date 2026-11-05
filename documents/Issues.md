# issues

## open

- Idempotency: a retried request whose first try reached the service but lost its response can be applied twice.
  Send an `Idempotency-Key` per job and have services de-duplicate (iam-service first).
- Optional keep-warm: a periodic health ping can keep a free-tier service awake, but it consumes the account's
  free instance hours (shared by all services); decide per service.

## resolved

- **On platform-db v0.2.5 (v0.1.7, 2026-11-05).** Keeps the platform's shared packages on one version each (FlashCards'
  start-up test now checks this). **Resolved:** `npm test` 32/32.
- **Several workers (v0.1.6, 2026-11-05).** `concurrency` (default 1) sends that many requests at once. With one
  worker, a request to a service still waking (a Render cold start takes 20–60 s, each try cut at the transport
  timeout) held up every other request, so callers behind it could time out; production saw a FlashCards login fail
  with 503 that way. Workers wake on new work, and the in-memory results are written by one flush at a time.
  **Resolved:** `npm test` 32/32. The new checks show one worker making a fast request wait behind a slow one,
  several workers not, and 12 requests on 4 workers each sent exactly once.
- **Per-request timeout (v0.1.5, 2026-11-04).** A request may carry its own `timeoutMs`; `HttpTransport` uses it
  instead of its default for that try. Needed for slow calls such as the IAM service's backup import (phase 3d), which
  must not be cut off at 15 s and retried while still running. Also on platform-db v0.2.4. **Resolved:** `npm test`
  29/29 (2 new checks).
- **platform-db v0.2.1 scheduling (v0.1.4, 2026-11-04).** Database work is scheduled in platform-db (`QueryScheduler`);
  the queue's store calls use a 1 s waiting limit so the worker never stalls inside one call. `TokenBucket` moved to
  platform-db. **Resolved:** `npm test` 27/27; on platform-db v0.2.0 (no short limit) the store's bookkeeping waited
  inside the scheduler and the "result kept in memory" check failed, which is what the limit fixes.
- **Outages of the queue's own database (v0.1.1, 2026-11-04).**
  - `start()` waits for the database with backoff (`QueueStoreUnavailableError` at the limit).
  - Table creation runs under platform-db's bootstrap lock.
  - The worker never dies on a store error; interactive requests keep flowing.
  - Results that couldn't be recorded are kept and written later, without resending.

  Before this, a database drop between sending and recording killed the worker loop silently (an unhandled rejection
  that could crash the process). **Resolved:** `npm test` 27/27 with outages simulated by a TCP relay, including
  exactly one send when the database dropped before the result was recorded, and no unhandled rejections.

- Published to GitHub (`trrmann/request-queue`, public); dependents use `git+https` tags.
