/**
 * RequestQueue test: a fake service that is asleep (not listening, then 503)
 * must not cause failures or lost requests; the rate limit must hold; durable
 * jobs must survive a queue restart; secrets must never reach the database; and the
 * queue must cope with its own database being down at start or dropping out mid-run.
 * Runs only against the local scratch database (local-db.sh).
 */

import { PostgresDatabase, LocalDatabaseTest, TcpRelay } from 'platform-db';
import { RequestQueue } from '../requestQueue.mjs';
import { HttpTransport } from '../httpTransport.mjs';
import { TokenBucket } from 'platform-db';
import { RetryPolicy } from '../retryPolicy.mjs';
import { PostgresQueueStore } from '../postgresQueueStore.mjs';
import { QueueTimeoutError, QueueStoreUnavailableError } from '../errors.mjs';
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

  /**
   * A queue whose store reaches the database through a TCP relay on `port`, so the test
   * can take "the database" away and bring it back.
   */
  async makeRelayedQueue(name, port, overrides = {}) {
    const database = new PostgresDatabase({
      ...this.database.config,
      port,
      connectionTimeoutMillis: 500,
    });
    this.databases.push(database);
    const store = new PostgresQueueStore({ database, schema: this.constructor.SCHEMA });
    return { store, queue: this.makeQueue({ name, durableStore: store, ...overrides }) };
  }

  relay() {
    return new TcpRelay({
      targetHost: this.database.config.host,
      targetPort: this.database.config.port,
    });
  }

  static async stateOf(promise) {
    const marker = Symbol('pending');
    try {
      return (await Promise.race([promise, Promise.resolve(marker)])) === marker
        ? 'pending'
        : 'resolved';
    } catch {
      return 'rejected';
    }
  }

  /** Poll `probe` until it returns true or `ms` pass. */
  static async waitUntil(probe, ms = 5000) {
    const until = Date.now() + ms;
    while (Date.now() < until) {
      if (await probe()) return true;
      await this.sleep(100);
    }
    return false;
  }

  async checkPerRequestTimeout() {
    console.log('\nPer-request timeout');
    const transport = new HttpTransport({ baseUrl: this.service.baseUrl, timeoutMs: 100 });
    this.service.beforeRespond = () => new Promise((resolve) => setTimeout(resolve, 300));
    try {
      let timedOut = null;
      try {
        await transport.send({ method: 'GET', path: '/slow' });
      } catch (error) {
        timedOut = error;
      }
      this.check(
        "a slow answer times out at the transport's limit",
        /timed out after 100 ms/.test(timedOut?.message ?? '')
      );
      const answer = await transport.send({ method: 'GET', path: '/slow', timeoutMs: 2000 });
      this.check("a request's own timeoutMs allows a slower answer", answer.status === 200);
    } finally {
      this.service.beforeRespond = null;
    }
  }

  /**
   * Several workers: a slow request (a service still waking) doesn't hold up the others, and
   * each job is still sent once.
   */
  async checkConcurrency() {
    console.log('\nConcurrency');
    const slowPaths = new Set(['/slow']);
    this.service.beforeRespond = async () => {
      const last = this.service.received[this.service.received.length - 1];
      if (slowPaths.has(last.path)) await new Promise((resolve) => setTimeout(resolve, 1500));
    };
    const make = (concurrency) =>
      new RequestQueue({
        name: `concurrency-${concurrency}`,
        transport: new HttpTransport({ baseUrl: this.service.baseUrl, timeoutMs: 5000 }),
        concurrency,
      });
    try {
      for (const concurrency of [1, 3]) {
        const queue = make(concurrency);
        await queue.start();
        try {
          const started = Date.now();
          const slow = queue.request({ method: 'GET', path: '/slow' });
          await new Promise((resolve) => setTimeout(resolve, 50));
          const fast = await queue.request({ method: 'GET', path: '/fast' });
          const fastMs = Date.now() - started;
          await slow;
          if (concurrency === 1) {
            this.check(
              'one worker: a fast request waits behind a slow one',
              fastMs >= 1400,
              `${fastMs} ms`
            );
          } else {
            this.check(
              'several workers: a fast request is not held up by a slow one',
              fast.status === 200 && fastMs < 1000,
              `${fastMs} ms`
            );
          }
        } finally {
          await queue.stop();
        }
      }
      const before = this.service.received.length;
      const queue = make(4);
      await queue.start();
      try {
        await Promise.all(
          Array.from({ length: 12 }, (_, i) => queue.request({ method: 'GET', path: `/many/${i}` }))
        );
      } finally {
        await queue.stop();
      }
      const paths = this.service.received.slice(before).map((r) => r.path);
      this.check(
        '12 requests on 4 workers: each sent exactly once',
        paths.length === 12 && new Set(paths).size === 12,
        String(paths.length)
      );
    } finally {
      this.service.beforeRespond = null;
    }
  }

  async checkStartWaitsForDatabase() {
    console.log('\nStart-up: the queue database is down at first');
    const port = await TcpRelay.freePort();
    const relay = this.relay();
    const { queue } = await this.makeRelayedQueue('late-db', port);
    try {
      const starting = queue.start();
      await this.constructor.sleep(700);
      this.check(
        'start() waits while the database is down',
        (await this.constructor.stateOf(starting)) === 'pending'
      );
      await relay.start(port);
      await starting;
      this.check('start() completes once the database answers', queue.running);
      const response = await queue.request(
        { method: 'POST', path: '/echo', body: { late: 1 } },
        { durable: true }
      );
      this.check('durable request delivered after a late start', response.status === 200);
    } finally {
      await queue.stop();
      await relay.stop();
    }

    const deadPort = await TcpRelay.freePort();
    const { queue: doomed } = await this.makeRelayedQueue('no-db', deadPort, {
      startTimeoutMs: 800,
    });
    let error = null;
    try {
      await doomed.start();
    } catch (caught) {
      error = caught;
    }
    this.check(
      'start() gives up with QueueStoreUnavailableError at the limit',
      error instanceof QueueStoreUnavailableError,
      error?.message
    );
  }

  async checkDatabaseDropMidRun() {
    console.log('\nThe queue database drops out while running');
    const port = await TcpRelay.freePort();
    const relay = this.relay();
    await relay.start(port);
    const { store, queue } = await this.makeRelayedQueue('drop', port);
    await queue.start();
    try {
      await this.service.close();
      const jobId = await queue.enqueue({ method: 'POST', path: '/echo', body: { drop: 1 } });
      await this.constructor.sleep(300); // claimed, sent, refused, rescheduled
      await relay.stop(); // the database goes away
      await this.service.listen();
      const interactive = await queue.request({
        method: 'POST',
        path: '/echo',
        body: { meanwhile: 1 },
      });
      this.check(
        'interactive requests keep flowing while the database is down',
        interactive.status === 200
      );
      let enqueueError = null;
      try {
        await queue.enqueue({ method: 'POST', path: '/echo', body: { drop: 2 } });
      } catch (caught) {
        enqueueError = caught;
      }
      this.check('enqueue() reports the outage to the caller', enqueueError !== null);
      await this.constructor.sleep(1000);
      this.check('worker is still running during the outage', queue.running);
      await relay.start(port); // the database is back
      const delivered = await this.constructor.waitUntil(
        async () => (await store.get(jobId))?.status === 'done'
      );
      this.check('stored job delivered after the database returns', delivered);
    } finally {
      await queue.stop();
      await relay.stop();
    }
  }

  async checkResultRecordedAfterOutage() {
    console.log('\nSent, then the database drops before the result is recorded');
    const port = await TcpRelay.freePort();
    const relay = this.relay();
    await relay.start(port);
    const { store, queue } = await this.makeRelayedQueue('record', port);
    await queue.start();
    try {
      this.service.beforeRespond = async () => {
        this.service.beforeRespond = null;
        await relay.stop(); // the service answers, but the queue can't record it
      };
      const before = this.service.received.filter((r) => r.body?.record).length;
      const jobId = await queue.enqueue({ method: 'POST', path: '/echo', body: { record: 1 } });
      await this.constructor.waitUntil(async () => queue.pendingRecords.length > 0, 3000);
      this.check(
        'result kept in memory while the database is down',
        queue.pendingRecords.length === 1
      );
      await this.constructor.sleep(500);
      await relay.start(port);
      const recorded = await this.constructor.waitUntil(
        async () => (await store.get(jobId))?.status === 'done'
      );
      this.check('result recorded once the database is back', recorded);
      const sends = this.service.received.filter((r) => r.body?.record).length - before;
      this.check('request was sent exactly once', sends === 1, String(sends));
    } finally {
      this.service.beforeRespond = null;
      await queue.stop();
      await relay.stop();
    }
  }

  async run() {
    this.assertLocalTarget();
    let unhandled = 0;
    const onUnhandled = (reason) => {
      unhandled += 1;
      console.error('Unhandled rejection:', reason);
    };
    process.on('unhandledRejection', onUnhandled);
    console.log('RequestQueue test (local scratch database)\n');
    await this.resetDatabase();
    await this.service.listen();
    try {
      await this.checkWaitsForSleepingService();
      await this.checkRateLimit();
      await this.checkDurability();
      await this.checkTimeoutAndSecrets();
      await this.checkPerRequestTimeout();
      await this.checkConcurrency();
      await this.checkStartWaitsForDatabase();
      await this.checkDatabaseDropMidRun();
      await this.checkResultRecordedAfterOutage();
      this.check('no unhandled promise rejections', unhandled === 0, String(unhandled));
    } finally {
      process.off('unhandledRejection', onUnhandled);
      await this.service.close();
      await this.resetDatabase();
    }
    return this.summary();
  }
}

export const requestQueueTest = new RequestQueueTest();
export default requestQueueTest;
