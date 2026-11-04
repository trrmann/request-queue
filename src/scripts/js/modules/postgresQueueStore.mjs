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

  async run(name, params = []) {
    return this.database.query(this.queries[name], params);
  }

  async initialize() {
    await this.run('create_schema');
    await this.run('create_table');
    await this.run('create_index');
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
