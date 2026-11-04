/**
 * Errors raised by the request queue.
 */

/** A send that did not get a usable response; `retryable` decides whether to try again. */
export class TransportError extends Error {
  constructor(message, { retryable = true, cause } = {}) {
    super(message, { cause });
    this.retryable = retryable;
  }
}

/** The caller stopped waiting. Durable jobs keep going; interactive ones are dropped. */
export class QueueTimeoutError extends Error {
  constructor(jobId, waitedMs) {
    super(`Request ${jobId} still queued after ${waitedMs} ms`);
    this.jobId = jobId;
    this.waitedMs = waitedMs;
  }
}

/** The durable store did not become ready within the start-up limit of RequestQueue.start(). */
export class QueueStoreUnavailableError extends Error {
  constructor(queueName, attempts, cause) {
    super(`Queue ${queueName}: store not ready after ${attempts} attempts: ${cause?.message}`, {
      cause,
    });
    this.name = 'QueueStoreUnavailableError';
    this.attempts = attempts;
  }
}
