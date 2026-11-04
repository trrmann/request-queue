/**
 * RequestQueue test entry point (npm test).
 * All behavior lives in the class-based RequestQueueTest module.
 */

import { requestQueueTest } from './requestQueueTest.mjs';

process.exitCode = (await requestQueueTest.execute()) ? 0 : 1;
