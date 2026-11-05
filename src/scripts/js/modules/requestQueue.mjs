/**
 * RequestQueue: delivers requests to a service that may be asleep (e.g. a
 * free-tier web service that takes ~50 s to wake), without losing them and
 * without failing while it wakes.
 *
 * - Durable requests are stored (e.g. PostgresQueueStore) and survive restarts;
 *   they are retried until delivered or older than the retry policy's maxAgeMs.
 * - Interactive requests (which may carry secrets, like a login) are kept only in
 *   memory; the caller awaits the result, up to `maxWaitMs`.
 * - Every send passes the rate limiter (token bucket).
 * - Network errors, timeouts, 429 and 502/503/504 are retried with exponential
 *   backoff; any other response is delivered to the caller as-is.
 *
 * Outages of the durable store (its database):
 * - start() waits for the store with backoff, up to `startTimeoutMs`.
 * - The worker never stops on a store error. Interactive requests keep flowing (they live
 *   in memory); durable work pauses with backoff and resumes when the store answers.
 * - If recording a result fails after the request was sent, the record is kept in memory
 *   and written when the store is back, so the request is not sent twice. Only if the
 *   process ends first is the job resent after the next start (at-least-once delivery):
 *   durable requests must be safe to repeat.
 * - enqueue() and durable request() fail straight away while the store is down; the caller
 *   decides what to do.
 */

import { MemoryQueueStore } from './memoryQueueStore.mjs';
import { TokenBucket } from 'platform-db';
import { RetryPolicy } from './retryPolicy.mjs';
import { QueueTimeoutError, QueueStoreUnavailableError, TransportError } from './errors.mjs';

export class RequestQueue {
  /**
   * @param {object} options
   * @param {string} options.name - Queue name (several queues can share a store)
   * @param {{send: Function}} options.transport - e.g. HttpTransport
   * @param {object} [options.durableStore] - Store for durable requests (default: memory)
   * @param {TokenBucket} [options.rateLimiter]
   * @param {RetryPolicy} [options.retryPolicy]
   * @param {number} [options.maxWaitMs] - How long callers wait by default
   * @param {number} [options.pollIntervalMs] - Idle wait between store checks
   * @param {number} [options.startTimeoutMs] - How long start() waits for the durable store
   * @param {number} [options.concurrency] - Requests sent at the same time (default 1). With
   *   more than one, a slow request (e.g. to a service that is still waking) doesn't hold up
   *   the others. Each job is still sent by one worker at a time.
   */
  constructor({
    name,
    transport,
    durableStore = new MemoryQueueStore(),
    rateLimiter = new TokenBucket(),
    retryPolicy = new RetryPolicy(),
    maxWaitMs = 120000,
    pollIntervalMs = 250,
    startTimeoutMs = 120000,
    concurrency = 1,
  }) {
    if (!name || !transport) throw new Error('RequestQueue needs a name and a transport');
    this.name = name;
    this.transport = transport;
    this.durableStore = durableStore;
    this.interactiveStore = new MemoryQueueStore();
    this.rateLimiter = rateLimiter;
    this.retryPolicy = retryPolicy;
    this.maxWaitMs = maxWaitMs;
    this.pollIntervalMs = pollIntervalMs;
    this.startTimeoutMs = startTimeoutMs;
    this.pendingRecords = []; // store writes that failed after a send: { store, method, args }
    this.storeFailures = 0; // consecutive durable-store failures
    this.storeRetryAt = 0; // no durable-store access before this time (backoff)
    this.waiters = new Map(); // job_id -> { resolve, reject, timer }
    this.concurrency = Math.max(1, Math.floor(concurrency));
    this.running = false;
    this.wakeSignals = new Set(); // one per idle worker
    this.loops = [];
    this.flushing = null; // the one flush of pendingRecords in progress
  }

  /**
   * Prepare the stores, recover jobs a previous worker left in progress, and start the worker.
   */
  async start() {
    if (this.running) return;
    await this.prepareDurableStore();
    this.running = true;
    this.loops = Array.from({ length: this.concurrency }, () => this.work());
  }

  /**
   * Initialize the durable store and recover jobs left in progress, retrying with backoff
   * while its database is unreachable. Throws QueueStoreUnavailableError after startTimeoutMs.
   */
  async prepareDurableStore() {
    const started = Date.now();
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.durableStore.initialize();
        await this.durableStore.releaseProcessing(this.name);
        if (attempt > 1) console.log(`Queue ${this.name}: store ready after ${attempt} attempts`);
        return;
      } catch (error) {
        const delay = this.retryPolicy.delayFor(attempt);
        if (Date.now() - started + delay > this.startTimeoutMs) {
          throw new QueueStoreUnavailableError(this.name, attempt, error);
        }
        if (this.constructor.shouldLog(attempt)) {
          console.warn(
            `Queue ${this.name}: store not ready (attempt ${attempt}): ${error.message}`
          );
        }
        await this.constructor.sleep(delay);
      }
    }
  }

  /** Log failure number 1, 2, 4, 8, … only, so a long outage doesn't flood the log. */
  static shouldLog(attempt) {
    return Number.isInteger(Math.log2(attempt));
  }

  static sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  /** Stop after the current send; durable jobs stay stored for the next start. */
  async stop() {
    this.running = false;
    this.wakeAll();
    await Promise.all(this.loops);
    try {
      await this.flushPendingRecords();
    } catch {
      console.warn(
        `Queue ${this.name}: ${this.pendingRecords.length} result(s) not recorded; ` +
          'those jobs are resent after the next start'
      );
    }
    for (const [jobId, waiter] of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error(`Queue ${this.name} stopped before request ${jobId} was delivered`));
    }
    this.waiters.clear();
  }

  /**
   * Queue a request and wait for its response.
   * @param {{method: string, path: string, body?: any, headers?: object}} request
   * @param {object} [options]
   * @param {boolean} [options.durable] - Store it so it survives restarts (no secrets!)
   * @param {number} [options.maxWaitMs] - Override how long to wait for the response
   * @returns {Promise<{status: number, body: any}>}
   */
  async request(request, { durable = false, maxWaitMs = this.maxWaitMs } = {}) {
    const store = durable ? this.durableStore : this.interactiveStore;
    const job = await store.enqueue(this.name, request);
    return this.awaitJob(job.job_id, durable, maxWaitMs);
  }

  /**
   * Queue a durable request without waiting (fire-and-forget); returns the job id.
   */
  async enqueue(request) {
    const job = await this.durableStore.enqueue(this.name, request);
    this.wake();
    return job.job_id;
  }

  awaitJob(jobId, durable, maxWaitMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(async () => {
        this.waiters.delete(jobId);
        if (!durable) await this.interactiveStore.remove(jobId); // nobody is waiting any more
        reject(new QueueTimeoutError(jobId, maxWaitMs));
      }, maxWaitMs);
      this.waiters.set(jobId, { resolve, reject, timer });
      this.wake();
    });
  }

  /** Wake one idle worker (new work arrived). */
  wake() {
    const [signal] = this.wakeSignals;
    if (signal) signal();
  }

  wakeAll() {
    for (const signal of [...this.wakeSignals]) signal();
  }

  idle(ms = this.pollIntervalMs) {
    return new Promise((resolve) => {
      const signal = () => {
        clearTimeout(timer);
        this.wakeSignals.delete(signal);
        resolve();
      };
      const timer = setTimeout(signal, ms);
      this.wakeSignals.add(signal);
    });
  }

  /** Interactive requests go first: someone is waiting on them, and they never need the database. */
  async claim() {
    const interactive = await this.interactiveStore.claimNext(this.name);
    if (interactive) return { job: interactive, store: this.interactiveStore };
    return this.claimDurable();
  }

  /**
   * Next due durable job, or null. While the store is failing, waits out the backoff; when
   * it answers again, first writes the results that could not be recorded earlier.
   */
  async claimDurable() {
    if (Date.now() < this.storeRetryAt) return null;
    try {
      await this.flushPendingRecords();
      const job = await this.durableStore.claimNext(this.name);
      this.storeRecovered();
      return job ? { job, store: this.durableStore } : null;
    } catch (error) {
      this.storeFailed(error);
      return null;
    }
  }

  /**
   * Write the results kept in memory while the store was down, oldest first. One flush at a
   * time: other workers wait for it, so no result is written twice.
   */
  flushPendingRecords() {
    if (!this.flushing) {
      this.flushing = (async () => {
        while (this.pendingRecords.length > 0) {
          const { store, method, args } = this.pendingRecords[0];
          await store[method](...args);
          this.pendingRecords.shift();
        }
      })().finally(() => {
        this.flushing = null;
      });
    }
    return this.flushing;
  }

  storeFailed(error) {
    this.storeFailures += 1;
    this.storeRetryAt = Date.now() + this.retryPolicy.delayFor(this.storeFailures);
    if (this.constructor.shouldLog(this.storeFailures)) {
      console.error(
        `Queue ${this.name}: store unavailable (failure ${this.storeFailures}): ${error.message}`
      );
    }
  }

  storeRecovered() {
    if (this.storeFailures === 0) return;
    console.log(`Queue ${this.name}: store available again after ${this.storeFailures} failure(s)`);
    this.storeFailures = 0;
    this.storeRetryAt = 0;
  }

  /**
   * Write a job's outcome to its store. If the store is down, keep the write for later
   * instead of failing (the request was already sent).
   */
  async record(store, method, args) {
    try {
      await store[method](...args);
    } catch (error) {
      this.pendingRecords.push({ store, method, args });
      this.storeFailed(error);
    }
  }

  /**
   * The worker loop. It never ends on an error: anything unexpected is logged and the loop
   * continues after a pause.
   */
  async work() {
    while (this.running) {
      try {
        const claimed = await this.claim();
        if (!claimed) {
          await this.idle();
          continue;
        }
        await this.rateLimiter.take();
        await this.deliver(claimed.job, claimed.store);
      } catch (error) {
        console.error(`Queue ${this.name}: unexpected worker error: ${error.message}`);
        await this.idle();
      }
    }
  }

  /**
   * Send one job and settle it: deliver the response, or reschedule a retry.
   */
  async deliver(job, store) {
    let outcome;
    try {
      outcome = await this.transport.send(job.request);
    } catch (error) {
      outcome = { error };
    }

    const retryable = outcome.error
      ? !(outcome.error instanceof TransportError) || outcome.error.retryable
      : this.retryPolicy.isRetryableStatus(outcome.status);

    // The caller is answered first; recording the outcome may have to wait for the store.
    if (!retryable) {
      this.settle(job.job_id, (waiter) => waiter.resolve(outcome));
      if (store === this.interactiveStore) {
        await store.remove(job.job_id);
      } else {
        await this.record(store, 'complete', [job.job_id, outcome]);
      }
      return;
    }

    const reason = outcome.error ? outcome.error.message : `HTTP ${outcome.status}`;
    if (this.retryPolicy.isExpired(job.created_at)) {
      this.settle(job.job_id, (waiter) =>
        waiter.reject(new Error(`Request gave up after retries: ${reason}`))
      );
      await this.record(store, 'fail', [job.job_id, `gave up: ${reason}`]);
      return;
    }
    const nextAttemptAt = new Date(Date.now() + this.retryPolicy.delayFor(job.attempts + 1));
    await this.record(store, 'reschedule', [job.job_id, nextAttemptAt, reason]);
  }

  settle(jobId, action) {
    const waiter = this.waiters.get(jobId);
    if (!waiter) return;
    clearTimeout(waiter.timer);
    this.waiters.delete(jobId);
    action(waiter);
  }
}

export default RequestQueue;
