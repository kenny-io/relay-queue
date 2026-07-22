/**
 * relay-queue — in-memory job queue with concurrency control, priorities,
 * timeouts, and retries with backoff.
 *
 * Jobs are async functions. The queue runs up to `concurrency` jobs at once,
 * picks the highest-priority waiting job first (FIFO within a priority), and
 * retries failed or timed-out jobs with configurable delay before reporting
 * failure. A job waiting on a retry delay still counts as running, so
 * `onIdle()` only resolves when work is truly finished.
 */

class TimeoutError extends Error {
  constructor(ms) {
    super(`job timed out after ${ms}ms`);
    this.name = "TimeoutError";
  }
}

/**
 * Create a new queue.
 *
 * @param {object} [options]
 * @param {number} [options.concurrency=1] - Max jobs running at once.
 * @param {number} [options.retries=0] - Retry attempts per job after the first failure.
 * @param {number} [options.timeoutMs] - Per-attempt time limit; a timed-out attempt counts as a failure and consumes a retry.
 * @param {number} [options.retryDelayMs=0] - Delay before the first retry.
 * @param {number} [options.backoffFactor=2] - Multiplier applied to the delay on each subsequent retry.
 * @param {(err: Error, jobId: number) => void} [options.onError] - Called when a job exhausts its retries.
 * @returns {object} queue
 */
export function createQueue(options = {}) {
  const concurrency = options.concurrency ?? 1;
  const retries = options.retries ?? 0;
  const defaultTimeoutMs = options.timeoutMs;
  const retryDelayMs = options.retryDelayMs ?? 0;
  const backoffFactor = options.backoffFactor ?? 2;
  const onError = options.onError ?? (() => {});

  const pending = [];
  let running = 0;
  let nextId = 1;
  let idleResolvers = [];
  let isPaused = false;
  const counters = { added: 0, completed: 0, failed: 0, retried: 0, timedOut: 0 };

  const settleIfIdle = () => {
    if (running === 0 && pending.length === 0) {
      idleResolvers.forEach((resolve) => resolve());
      idleResolvers = [];
    }
  };

  /** Highest priority first; insertion order within equal priorities. */
  const insertByPriority = (job) => {
    let index = pending.length;
    while (index > 0 && pending[index - 1].priority < job.priority) index -= 1;
    pending.splice(index, 0, job);
  };

  const attemptOnce = (job) => {
    const timeoutMs = job.timeoutMs ?? defaultTimeoutMs;
    const run = Promise.resolve().then(job.fn);
    if (!timeoutMs) return run;
    let timer;
    return Promise.race([
      run.finally(() => clearTimeout(timer)),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          counters.timedOut += 1;
          reject(new TimeoutError(timeoutMs));
        }, timeoutMs);
        if (typeof timer.unref === "function") timer.unref();
      }),
    ]);
  };

  const runNext = () => {
    if (isPaused || running >= concurrency || pending.length === 0) {
      settleIfIdle();
      return;
    }
    const job = pending.shift();
    running += 1;

    const attempt = (remaining, delayMs) =>
      attemptOnce(job).catch((err) => {
        if (remaining > 0) {
          counters.retried += 1;
          if (delayMs > 0) {
            return new Promise((resolve) => {
              const timer = setTimeout(resolve, delayMs);
              if (typeof timer.unref === "function") timer.unref();
            }).then(() => attempt(remaining - 1, delayMs * backoffFactor));
          }
          return attempt(remaining - 1, delayMs * backoffFactor);
        }
        counters.failed += 1;
        onError(err, job.id);
        return "__relay_failed__";
      });

    attempt(retries, retryDelayMs)
      .then((result) => {
        if (result !== "__relay_failed__") counters.completed += 1;
      })
      .finally(() => {
        running -= 1;
        runNext();
      });
    runNext();
  };

  return {
    /**
     * Add a job to the queue.
     * @param {() => Promise<any>} fn - Async job function.
     * @param {object} [jobOptions]
     * @param {number} [jobOptions.priority=0] - Higher runs first; FIFO within a priority.
     * @param {number} [jobOptions.timeoutMs] - Per-attempt time limit for this job.
     * @returns {number} job id
     */
    add(fn, jobOptions = {}) {
      const id = nextId++;
      counters.added += 1;
      insertByPriority({
        id,
        fn,
        priority: jobOptions.priority ?? 0,
        timeoutMs: jobOptions.timeoutMs,
      });
      queueMicrotask(runNext);
      return id;
    },

    /**
     * Enqueue several jobs at once.
     * @param {Array<() => Promise<any>>} fns
     * @returns {Array<number>} job ids in input order
     */
    addAll(fns) {
      return fns.map((fn) => this.add(fn));
    },

    /** Number of jobs waiting (not counting running jobs). */
    size() {
      return pending.length;
    },

    /** Number of jobs currently running (including retry delays). */
    running() {
      return running;
    },

    /**
     * Lifetime counters plus current state.
     * @returns {{ added: number, completed: number, failed: number, retried: number, timedOut: number, running: number, pending: number }}
     */
    stats() {
      return { ...counters, running, pending: pending.length };
    },

    /** Resolves when the queue is fully drained. */
    onIdle() {
      if (running === 0 && pending.length === 0) return Promise.resolve();
      return new Promise((resolve) => idleResolvers.push(resolve));
    },

    /** Stop starting new jobs; running jobs finish normally. */
    pause() {
      isPaused = true;
    },

    /** Resume starting jobs after a pause. */
    resume() {
      if (!isPaused) return;
      isPaused = false;
      queueMicrotask(runNext);
    },

    /**
     * Drop all waiting jobs without running them. Running jobs finish.
     * @returns {number} how many jobs were dropped
     */
    clear() {
      const dropped = pending.length;
      pending.length = 0;
      queueMicrotask(runNext);
      return dropped;
    },

    /** Whether the queue is currently paused. */
    isPaused() {
      return isPaused;
    },
  };
}
