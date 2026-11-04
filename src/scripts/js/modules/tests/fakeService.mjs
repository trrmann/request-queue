/**
 * A fake HTTP service for queue tests that can be "asleep": either not listening
 * at all (connection refused, like a stopped instance) or answering 503 (like a
 * proxy in front of an instance that is still starting). Records every request.
 *
 * Routes: POST /echo → 200 { echo: body }; GET /bad → 400.
 */

import http from 'node:http';

export class FakeService {
  constructor(port) {
    this.port = port;
    this.server = null;
    this.awakeAt = 0;
    this.received = [];
  }

  get baseUrl() {
    return `http://127.0.0.1:${this.port}`;
  }

  /** Answer 503 until `ms` from now. */
  sleepFor(ms) {
    this.awakeAt = Date.now() + ms;
  }

  async listen() {
    if (this.server) return;
    this.server = http.createServer((req, res) => this.handle(req, res));
    await new Promise((resolve) => this.server.listen(this.port, '127.0.0.1', resolve));
  }

  async close() {
    if (!this.server) return;
    const server = this.server;
    this.server = null;
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }

  async readBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString();
    return text ? JSON.parse(text) : null;
  }

  async handle(req, res) {
    const body = await this.readBody(req);
    const at = Date.now();
    if (at < this.awakeAt) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'error', message: 'starting' }));
      return;
    }
    this.received.push({ method: req.method, path: req.url, body, at });
    if (req.url === '/bad') {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'error', message: 'bad request' }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'success', echo: body }));
  }
}

export default FakeService;
