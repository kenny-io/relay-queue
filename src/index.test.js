import test from "node:test";
import assert from "node:assert/strict";

import { createQueue } from "./index.js";

const tick = (ms = 0) => new Promise((resolve) => setTimeout(resolve, ms));

test("runs jobs with bounded concurrency", async () => {
  const queue = createQueue({ concurrency: 2 });
  let peak = 0;
  let active = 0;
  const job = () => {
    active += 1;
    peak = Math.max(peak, active);
    return tick(20).then(() => {
      active -= 1;
    });
  };

  queue.addAll([job, job, job, job, job]);
  await queue.onIdle();
  assert.equal(peak, 2);
  assert.equal(queue.stats().completed, 5);
});

test("higher priority jobs run first, FIFO within a priority", async () => {
  const queue = createQueue({ concurrency: 1 });
  const order = [];
  queue.pause();
  queue.add(() => order.push("low-1"));
  queue.add(() => order.push("low-2"));
  queue.add(() => order.push("high"), { priority: 10 });
  queue.add(() => order.push("mid"), { priority: 5 });
  queue.resume();

  await queue.onIdle();
  assert.deepEqual(order, ["high", "mid", "low-1", "low-2"]);
});

test("timeouts count as failures and consume retries", async () => {
  const failures = [];
  const queue = createQueue({
    timeoutMs: 25,
    retries: 1,
    onError: (err, id) => failures.push({ name: err.name, id }),
  });

  let attempts = 0;
  queue.add(() => {
    attempts += 1;
    return tick(200); // never finishes inside the timeout
  });

  await queue.onIdle();
  assert.equal(attempts, 2);
  assert.deepEqual(failures, [{ name: "TimeoutError", id: 1 }]);
  assert.equal(queue.stats().timedOut, 2);
  assert.equal(queue.stats().failed, 1);
});

test("retries back off and eventually succeed", async () => {
  const queue = createQueue({ retries: 3, retryDelayMs: 10, backoffFactor: 2 });
  let attempts = 0;
  const startedAt = Date.now();

  queue.add(() => {
    attempts += 1;
    if (attempts < 3) throw new Error("flaky");
  });

  await queue.onIdle();
  const elapsed = Date.now() - startedAt;
  assert.equal(attempts, 3);
  // Two retries: 10ms + 20ms of backoff at minimum.
  assert.ok(elapsed >= 25, `expected backoff delays, finished in ${elapsed}ms`);
  assert.equal(queue.stats().retried, 2);
  assert.equal(queue.stats().completed, 1);
});

test("onIdle waits through retry delays", async () => {
  const queue = createQueue({ retries: 1, retryDelayMs: 30 });
  let done = false;
  queue.add(() => {
    if (!done) {
      done = true;
      throw new Error("first try fails");
    }
  });

  await queue.onIdle();
  assert.equal(queue.stats().completed, 1);
  assert.equal(queue.running(), 0);
});

test("stats, clear and pause interplay", async () => {
  const queue = createQueue({ concurrency: 1 });
  queue.pause();
  queue.addAll([() => tick(5), () => tick(5), () => tick(5)]);

  assert.equal(queue.stats().pending, 3);
  assert.equal(queue.clear(), 3);
  queue.resume();
  await queue.onIdle();

  const stats = queue.stats();
  assert.equal(stats.added, 3);
  assert.equal(stats.completed, 0);
});
