/**
 * In-memory queue store: same interface as PostgresQueueStore, for interactive
 * requests (which may carry secrets and must never be persisted) and for tests.
 * Contents are lost when the process ends.
 */

import crypto from 'node:crypto';

export class MemoryQueueStore {
  constructor() {
    this.jobs = new Map();
  }

  async initialize() {}

  async enqueue(queueName, request) {
    const job = {
      job_id: crypto.randomUUID(),
      queue_name: queueName,
      request,
      status: 'pending',
      attempts: 0,
      next_attempt_at: new Date(),
      last_error: null,
      response: null,
      created_at: new Date(),
    };
    this.jobs.set(job.job_id, job);
    return { ...job };
  }

  async claimNext(queueName) {
    const now = Date.now();
    const due = [...this.jobs.values()]
      .filter(
        (j) =>
          j.queue_name === queueName && j.status === 'pending' && j.next_attempt_at.getTime() <= now
      )
      .sort((a, b) => a.created_at - b.created_at)[0];
    if (!due) return null;
    due.status = 'processing';
    return { ...due };
  }

  update(jobId, changes) {
    const job = this.jobs.get(jobId);
    if (job) Object.assign(job, changes);
  }

  async reschedule(jobId, nextAttemptAt, error) {
    const job = this.jobs.get(jobId);
    if (job)
      this.update(jobId, {
        status: 'pending',
        attempts: job.attempts + 1,
        next_attempt_at: nextAttemptAt,
        last_error: error,
      });
  }

  async complete(jobId, response) {
    this.update(jobId, { status: 'done', response });
  }

  async fail(jobId, error) {
    this.update(jobId, { status: 'failed', last_error: error });
  }

  async remove(jobId) {
    this.jobs.delete(jobId);
  }

  async releaseProcessing(queueName) {
    for (const job of this.jobs.values()) {
      if (job.queue_name === queueName && job.status === 'processing') job.status = 'pending';
    }
  }

  async get(jobId) {
    const job = this.jobs.get(jobId);
    return job ? { ...job } : null;
  }
}

export default MemoryQueueStore;
