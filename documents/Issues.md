# issues

## open

- **Priority classes and database-work scheduling (owner request, 2026-11-04; after the FlashCards release).**
  platform-db will route its queries through a scheduler built on this package (concurrency cap, rate limit, priority
  classes from a rules file, ageing). That needs a generic job transport (a function, not only HTTP) and priorities
  in `claimNext`. Design: iam-service `documents/PLATFORM_ACCESS_AND_STARTUP.md`.

- Idempotency: a retried request whose first try reached the service but lost its response can be applied twice.
  Send an `Idempotency-Key` per job and have services de-duplicate (iam-service first).
- Optional keep-warm: a periodic health ping can keep a free-tier service awake, but it consumes the account's
  free instance hours (shared by all services); decide per service.

## resolved

- **Outages of the queue's own database (v0.1.1, 2026-11-04).**
  - `start()` waits for the database with backoff (`QueueStoreUnavailableError` at the limit).
  - Table creation runs under platform-db's bootstrap lock.
  - The worker never dies on a store error; interactive requests keep flowing.
  - Results that couldn't be recorded are kept and written later, without resending.

  Before this, a database drop between sending and recording killed the worker loop silently (an unhandled rejection
  that could crash the process). **Resolved:** `npm test` 27/27 with outages simulated by a TCP relay, including
  exactly one send when the database dropped before the result was recorded, and no unhandled rejections.

- Published to GitHub (`trrmann/request-queue`, public); dependents use `git+https` tags.
