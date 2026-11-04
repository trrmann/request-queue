# issues

## open

- Idempotency: a retried request whose first try reached the service but lost its response can be applied twice.
  Send an `Idempotency-Key` per job and have services de-duplicate (iam-service first).
- Optional keep-warm: a periodic health ping can keep a free-tier service awake, but it consumes the account's
  free instance hours (shared by all services); decide per service.

## resolved

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
