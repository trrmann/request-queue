/**
 * Retry policy: exponential backoff with jitter for responses that mean
 * "the service is not ready yet" (network errors, timeouts, 502/503/504, 429).
 */

export class RetryPolicy {
  static RETRYABLE_STATUSES = [429, 502, 503, 504];

  constructor({
    baseDelayMs = 500,
    factor = 2,
    maxDelayMs = 30000,
    jitter = 0.2,
    maxAgeMs = 24 * 60 * 60 * 1000,
  } = {}) {
    this.baseDelayMs = baseDelayMs;
    this.factor = factor;
    this.maxDelayMs = maxDelayMs;
    this.jitter = jitter;
    this.maxAgeMs = maxAgeMs;
  }

  /** Delay before the next try, after `attempts` failed tries. */
  delayFor(attempts) {
    const delay = Math.min(
      this.maxDelayMs,
      this.baseDelayMs * this.factor ** Math.max(0, attempts - 1)
    );
    const spread = delay * this.jitter;
    return Math.round(delay - spread + Math.random() * spread * 2);
  }

  isRetryableStatus(status) {
    return RetryPolicy.RETRYABLE_STATUSES.includes(status);
  }

  /** A job older than maxAgeMs is given up (marked failed, kept for inspection). */
  isExpired(createdAt) {
    return Date.now() - new Date(createdAt).getTime() > this.maxAgeMs;
  }
}

export default RetryPolicy;
