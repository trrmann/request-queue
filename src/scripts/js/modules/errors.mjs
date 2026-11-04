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
