# request-queue

A durable, rate-limited request queue for calling services that may be asleep, such as a free-tier web
service that takes about 50 seconds to wake. It is a **library**, not a service: each application runs its
own queue, and durable jobs live in that application's schema of the always-on shared database.

## Behavior

| Concern                       | How                                                                                                                                                                                                                                                              |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Never lose a request          | **Durable** requests are stored (`PostgresQueueStore`, table `<schema>.request_queue`) and survive restarts; a restarted queue resumes them                                                                                                                      |
| Never fail while waking       | Network errors, timeouts, `429`, `502`, `503`, `504` are retried with exponential backoff and jitter (`RetryPolicy`); other responses are delivered as-is                                                                                                        |
| Rate limit                    | Every send takes a token from a `TokenBucket` (burst capacity + refill per second)                                                                                                                                                                               |
| Secrets                       | **Interactive** requests (logins, token refreshes) are kept in memory only; credentials are added by the transport at send time and never stored                                                                                                                 |
| Caller waits                  | `request()` resolves with `{ status, body }`; after `maxWaitMs` the caller gets `QueueTimeoutError` (durable jobs keep going, interactive ones are dropped)                                                                                                      |
| Order                         | Oldest due job first; interactive requests before durable ones                                                                                                                                                                                                   |
| Giving up                     | A durable job older than `RetryPolicy.maxAgeMs` (default 24 h) is marked `failed` and kept for inspection                                                                                                                                                        |
| Queue database down at start  | `start()` creates the queue table (under platform-db's bootstrap lock) and recovers unfinished jobs, retrying with backoff; after `startTimeoutMs` (default 120 s) it throws `QueueStoreUnavailableError`                                                        |
| Queue database drops out      | The worker never stops. Interactive requests keep flowing (memory only); durable work pauses with backoff and resumes when the database answers. Failures 1, 2, 4, 8 … are logged, then one line on recovery                                                     |
| Sent, but result not recorded | The result is kept in memory and written when the database is back, so the request isn't sent twice. If the process ends first, the job is resent after the next start (**at-least-once**): durable requests must be safe to repeat until idempotency keys exist |
| Database scheduling           | Store queries run through platform-db's `QueryScheduler` with a 1 s waiting limit (`PostgresQueueStore.QUERY_WAIT_MS`), so they fail fast and the queue's own backoff takes over. `TokenBucket` now lives in platform-db (re-exported here)                      |
| Enqueue during an outage      | `enqueue()` and durable `request()` throw straight away; the caller decides (retry later, or report)                                                                                                                                                             |

## Usage

```js
import { RequestQueue, HttpTransport, PostgresQueueStore, TokenBucket } from 'request-queue';

const queue = new RequestQueue({
  name: 'iam',
  transport: new HttpTransport({ baseUrl: process.env.IAM_URL, headers: () => credentials() }),
  durableStore: new PostgresQueueStore({ database: appDb, schema: 'flashcards' }),
  rateLimiter: new TokenBucket({ capacity: 10, refillPerSecond: 5 }),
});
await queue.start();
const login = await queue.request({ method: 'POST', path: '/v1/auth/login', body }); // interactive
await queue.enqueue({ method: 'PUT', path: '/v1/...', body }); // durable, fire-and-forget
```

Depends on [platform-db](../../platform-db/documents/README.md). Tests: `npm test` (local scratch database):

- a fake service that is asleep (not listening, then answering 503);
- the queue's own database down at start, giving up at the limit, and dropping out mid-run (TCP relay from
  platform-db);
- a result recorded after an outage without a second send;
- no unhandled promise rejections.

See [CODING_STANDARDS.md](./CODING_STANDARDS.md) and [Issues.md](./Issues.md).
