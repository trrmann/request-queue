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
 */

import { MemoryQueueStore } from './memoryQueueStore.mjs';
import { TokenBucket } from './tokenBucket.mjs';
import { RetryPolicy } from './retryPolicy.mjs';
import { QueueTimeoutError, TransportError } from './errors.mjs';

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
   */
  constructor({
    name,
    transport,
    durableStore = new MemoryQueueStore(),
    rateLimiter = new TokenBucket(),
    retryPolicy = new RetryPolicy(),
    maxWaitMs = 120000,
    pollIntervalMs = 250,
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
    this.waiters = new Map(); // job_id -> { resolve, reject, timer }
    this.running = false;
    this.wakeSignal = null;
    this.loop = null;
  }

  /**
   * Prepare the stores, recover jobs a previous worker left in progress, and start the worker.
   */
  async start() {
    if (this.running) return;
    await this.durableStore.initialize();
    await this.durableStore.releaseProcessing(this.name);
    this.running = true;
    this.loop = this.work();
  }

  /** Stop after the current send; durable jobs stay stored for the next start. */
  async stop() {
    this.running = false;
    this.wake();
    await this.loop;
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

  wake() {
    if (this.wakeSignal) {
      const signal = this.wakeSignal;
      this.wakeSignal = null;
      signal();
    }
  }

  idle() {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, this.pollIntervalMs);
      this.wakeSignal = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  /** Interactive requests go first: someone is waiting on them. */
  async claim() {
    const interactive = await this.interactiveStore.claimNext(this.name);
    if (interactive) return { job: interactive, store: this.interactiveStore };
    const durable = await this.durableStore.claimNext(this.name);
    return durable ? { job: durable, store: this.durableStore } : null;
  }

  async work() {
    while (this.running) {
      let claimed = null;
      try {
        claimed = await this.claim();
      } catch (error) {
        console.error(`Queue ${this.name}: store unavailable: ${error.message}`);
      }
      if (!claimed) {
        await this.idle();
        continue;
      }
      await this.rateLimiter.take();
      await this.deliver(claimed.job, claimed.store);
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

    if (!retryable) {
      await store.complete(job.job_id, outcome);
      this.settle(job.job_id, (waiter) => waiter.resolve(outcome));
      if (store === this.interactiveStore) await store.remove(job.job_id);
      return;
    }

    const reason = outcome.error ? outcome.error.message : `HTTP ${outcome.status}`;
    if (this.retryPolicy.isExpired(job.created_at)) {
      await store.fail(job.job_id, `gave up: ${reason}`);
      this.settle(job.job_id, (waiter) =>
        waiter.reject(new Error(`Request gave up after retries: ${reason}`))
      );
      return;
    }
    const nextAttemptAt = new Date(Date.now() + this.retryPolicy.delayFor(job.attempts + 1));
    await store.reschedule(job.job_id, nextAttemptAt, reason);
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
