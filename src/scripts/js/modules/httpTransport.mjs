/**
 * HTTP transport for the request queue. Sends a queued request
 * `{ method, path, body }` to `baseUrl`, adding headers at send time (so
 * credentials are never stored with the request), with a per-try timeout: the transport's,
 * or the request's own `timeoutMs` (for slow calls such as backups).
 * Network errors and timeouts throw a retryable TransportError; any HTTP
 * response is returned as `{ status, body }` for the queue to classify.
 */

import { TransportError } from './errors.mjs';

export class HttpTransport {
  constructor({ baseUrl, headers = () => ({}), timeoutMs = 15000 }) {
    if (!baseUrl) throw new Error('HttpTransport needs a baseUrl');
    this.baseUrl = String(baseUrl).replace(/\/+$/, '');
    this.headers = headers;
    this.timeoutMs = timeoutMs;
  }

  async send({ method = 'GET', path = '/', body, headers = {}, timeoutMs }) {
    const limitMs = timeoutMs ?? this.timeoutMs;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), limitMs);
    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
          ...(await this.headers()),
          ...headers,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
      const text = await response.text();
      let parsed = text;
      try {
        parsed = text ? JSON.parse(text) : null;
      } catch {
        // not JSON: keep the text
      }
      return { status: response.status, body: parsed };
    } catch (error) {
      const reason = error.name === 'AbortError' ? `timed out after ${limitMs} ms` : error.message;
      throw new TransportError(`${method} ${path}: ${reason}`, { retryable: true, cause: error });
    } finally {
      clearTimeout(timer);
    }
  }
}

export default HttpTransport;
