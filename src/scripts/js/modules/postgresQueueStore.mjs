/**
 * Durable queue store on PostgreSQL (platform-db). Jobs live in
 * <schema>.request_queue of the owning application's schema, so queued work
 * survives restarts and outages of the services it calls. Only store requests
 * without secrets here; credentials are added by the transport at send time.
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PostgresDatabase } from 'platform-db';

export class PostgresQueueStore {
  static IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/;
  /** Longest a store query may wait in platform-db's scheduler (queue + retries). */
  static QUERY_WAIT_MS = 1000;
  static sqlRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../sql');
  static templates = PostgresDatabase.loadNamedSqlStatements(
    'pg/queue-queries.sql',
    PostgresQueueStore.sqlRoot
  );

  /**
   * @param {object} options
   * @param {PostgresDatabase} options.database - The owning application's adapter
   * @param {string} options.schema - Schema for the queue table (e.g. 'flashcards')
   */
  constructor({ database, schema }) {
    if (!database) throw new Error('PostgresQueueStore needs a database adapter');
    if (!PostgresQueueStore.IDENTIFIER.test(String(schema))) {
      throw new Error(`Invalid schema name for the queue: ${schema}`);
    }
    this.database = database;
    this.schema = schema;
    this.queries = Object.fromEntries(
      Object.entries(PostgresQueueStore.templates).map(([name, sql]) => [
        name,
        sql.replaceAll('{{schema}}', schema),
      ])
    );
  }

  /**
   * Run a named store query. The queue has its own outage handling (backoff, results kept
   * in memory), so store calls fail fast instead of waiting out the scheduler's class limit.
   */
  async run(name, params = []) {
    return this.database.query(this.queries[name], params, {
      maxWaitMs: PostgresQueueStore.QUERY_WAIT_MS,
    });
  }

  /**
   * Create the queue's schema, table and index (idempotent). Runs on one connection under
   * platform-db's bootstrap lock, so it never overlaps another process's schema bootstrap.
   * Throws while the database is unreachable; RequestQueue.start() retries it.
   */
  async initialize() {
    const locks = PostgresDatabase.lockQueries;
    const key = PostgresDatabase.BOOTSTRAP_LOCK_KEY;
    const client = await this.database.getClient();
    try {
      await client.query(locks.acquire_advisory_lock, [key]);
      for (const name of ['create_schema', 'create_table', 'create_index']) {
        await client.query(this.queries[name]);
      }
    } finally {
      await client.query(locks.release_advisory_lock, [key]).catch(() => {});
      client.release();
    }
  }

  async enqueue(queueName, request) {
    return (await this.run('insert_job', [queueName, request])).rows[0];
  }

  async claimNext(queueName) {
    return (await this.run('claim_next', [queueName])).rows[0] || null;
  }

  async reschedule(jobId, nextAttemptAt, error) {
    await this.run('reschedule_job', [jobId, nextAttemptAt, error]);
  }

  async complete(jobId, response) {
    await this.run('complete_job', [jobId, response]);
  }

  async fail(jobId, error) {
    await this.run('fail_job', [jobId, error]);
  }

  async remove(jobId) {
    await this.run('delete_job', [jobId]);
  }

  async releaseProcessing(queueName) {
    await this.run('release_processing', [queueName]);
  }

  async get(jobId) {
    return (await this.run('get_job', [jobId])).rows[0] || null;
  }

  /** Tests only: remove the queue's schema. */
  async dropSchema() {
    await this.run('drop_schema');
  }
}

export default PostgresQueueStore;
