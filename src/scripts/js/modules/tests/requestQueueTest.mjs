/**
 * RequestQueue test: a fake service that is asleep (not listening, then 503)
 * must not cause failures or lost requests; the rate limit must hold; durable
 * jobs must survive a queue restart; secrets must never reach the database.
 * Runs only against the local scratch database (local-db.sh).
 */

import { PostgresDatabase, LocalDatabaseTest } from 'platform-db';
import { RequestQueue } from '../requestQueue.mjs';
import { HttpTransport } from '../httpTransport.mjs';
import { TokenBucket } from '../tokenBucket.mjs';
import { RetryPolicy } from '../retryPolicy.mjs';
import { PostgresQueueStore } from '../postgresQueueStore.mjs';
import { QueueTimeoutError } from '../errors.mjs';
import { FakeService } from './fakeService.mjs';

export class RequestQueueTest extends LocalDatabaseTest {
  static PORT = 5199;
  static SCHEMA = 'queue_test';
  static testTemplates = PostgresDatabase.loadNamedSqlStatements(
    'pg/test-queries.sql',
    PostgresQueueStore.sqlRoot
  );

  constructor(database = new PostgresDatabase()) {
    super([database]);
    this.database = database;
    this.service = new FakeService(this.constructor.PORT);
    this.store = new PostgresQueueStore({ database, schema: this.constructor.SCHEMA });
    this.testQueries = Object.fromEntries(
      Object.entries(this.constructor.testTemplates).map(([name, sql]) => [
        name,
        sql.replaceAll('{{schema}}', this.constructor.SCHEMA),
      ])
    );
  }

  async resetDatabase() {
    await this.store.dropSchema();
  }

  /** A fast queue for tests: short backoff, generous rate. */
  makeQueue(overrides = {}) {
    return new RequestQueue({
      name: 'test',
      transport: new HttpTransport({ baseUrl: this.service.baseUrl, timeoutMs: 1000 }),
      durableStore: this.store,
      rateLimiter: new TokenBucket({ capacity: 50, refillPerSecond: 100 }),
      retryPolicy: new RetryPolicy({ baseDelayMs: 100, maxDelayMs: 300 }),
      pollIntervalMs: 50,
      ...overrides,
    });
  }

  async count(name, params = []) {
    return (await this.database.query(this.testQueries[name], params)).rows[0].count;
  }

  static sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  async checkWaitsForSleepingService() {
    console.log('Waiting for a sleeping service');
    const queue = this.makeQueue();
    await queue.start();
    try {
      await this.service.close(); // stopped instance: connection refused
      const started = Date.now();
      const pending = queue.request({ method: 'POST', path: '/echo', body: { n: 1 } });
      setTimeout(() => this.service.listen(), 1500);
      const response = await pending;
      const waited = Date.now() - started;
      this.check(
        'request survives connection-refused and is delivered',
        response.status === 200 && response.body?.echo?.n === 1
      );
      this.check('it waited for the service to start (≥ 1.5 s)', waited >= 1400, `${waited} ms`);

      this.service.sleepFor(1000); // starting instance: 503 from the proxy
      const second = await queue.request({ method: 'POST', path: '/echo', body: { n: 2 } });
      this.check(
        'request survives 503 responses and is delivered',
        second.status === 200 && second.body?.echo?.n === 2
      );

      const before = this.service.received.length;
      const bad = await queue.request({ method: 'GET', path: '/bad' });
      this.check(
        '400 is delivered as-is (not retried)',
        bad.status === 400 && this.service.received.length === before + 1
      );
    } finally {
      await queue.stop();
    }
  }

  async checkRateLimit() {
    console.log('\nRate limit');
    const queue = this.makeQueue({
      rateLimiter: new TokenBucket({ capacity: 2, refillPerSecond: 4 }),
    });
    await queue.start();
    try {
      const firstIndex = this.service.received.length;
      const started = Date.now();
      const responses = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          queue.request({ method: 'POST', path: '/echo', body: { i } })
        )
      );
      const elapsed = Date.now() - started;
      this.check(
        'all 10 burst requests delivered',
        responses.every((r) => r.status === 200)
      );
      this.check('burst of 10 at 2 + 4/s took ≥ 2 s', elapsed >= 1900, `${elapsed} ms`);
      const times = this.service.received.slice(firstIndex).map((r) => r.at);
      const maxInWindow = Math.max(
        ...times.map((t) => times.filter((u) => u >= t && u < t + 1000).length)
      );
      this.check(
        'never more than 6 requests in any 1 s window',
        maxInWindow <= 6,
        String(maxInWindow)
      );
      this.check(
        'requests delivered oldest first',
        this.service.received.slice(firstIndex).every((r, index) => r.body.i === index)
      );
    } finally {
      await queue.stop();
    }
  }

  async checkDurability() {
    console.log('\nDurable jobs survive a restart');
    await this.service.close();
    const first = this.makeQueue();
    await first.start();
    const ids = [];
    for (const n of [1, 2, 3]) {
      ids.push(await first.enqueue({ method: 'POST', path: '/echo', body: { durable: n } }));
    }
    await this.constructor.sleep(400);
    await first.stop();
    this.check(
      '3 durable jobs stored while the service is down',
      (await this.count('count_jobs_with_status', ['pending'])) === 3
    );

    await this.service.listen();
    const second = this.makeQueue();
    await second.start();
    try {
      for (
        let i = 0;
        i < 40 && (await this.count('count_jobs_with_status', ['done'])) < 3;
        i += 1
      ) {
        await this.constructor.sleep(100);
      }
      this.check(
        'a new queue delivered all 3 stored jobs',
        (await this.count('count_jobs_with_status', ['done'])) === 3
      );
      const delivered = this.service.received
        .filter((r) => r.body?.durable)
        .map((r) => r.body.durable);
      this.check(
        'each stored job delivered exactly once',
        delivered.sort().join(',') === '1,2,3',
        delivered.join(',')
      );
      const job = await this.store.get(ids[0]);
      this.check('job keeps its response', job?.response?.status === 200 && job.attempts >= 1);
    } finally {
      await second.stop();
    }
  }

  async checkTimeoutAndSecrets() {
    console.log('\nCaller timeout and secrets');
    const queue = this.makeQueue();
    await queue.start();
    try {
      await this.service.close();
      let error = null;
      try {
        await queue.request(
          { method: 'POST', path: '/echo', body: { password: 'never-store-me' } },
          { maxWaitMs: 500 }
        );
      } catch (caught) {
        error = caught;
      }
      this.check('interactive caller gets QueueTimeoutError', error instanceof QueueTimeoutError);
      this.check(
        'timed-out interactive request is dropped',
        queue.interactiveStore.jobs.size === 0
      );
      this.check(
        'interactive requests never reach the database',
        (await this.count('count_jobs_containing', ['never-store-me'])) === 0
      );
    } finally {
      await queue.stop();
      await this.service.listen();
    }
  }

  async run() {
    this.assertLocalTarget();
    console.log('RequestQueue test (local scratch database)\n');
    await this.resetDatabase();
    await this.service.listen();
    try {
      await this.checkWaitsForSleepingService();
      await this.checkRateLimit();
      await this.checkDurability();
      await this.checkTimeoutAndSecrets();
    } finally {
      await this.service.close();
      await this.resetDatabase();
    }
    return this.summary();
  }
}

export const requestQueueTest = new RequestQueueTest();
export default requestQueueTest;
