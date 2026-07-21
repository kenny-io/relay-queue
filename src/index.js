/**
 * relay-queue — in-memory job queue with concurrency control and retries.
 *
 * Jobs are async functions. The queue runs up to `concurrency` jobs at once
 * and retries failed jobs up to `retries` times before reporting failure.
 */

/**
 * Create a new queue.
 *
 * @param {object} [options]
 * @param {number} [options.concurrency=1] - Max jobs running at once.
 * @param {number} [options.retries=0] - Retry attempts per job after the first failure.
 * @param {(err: Error, jobId: number) => void} [options.onError] - Called when a job exhausts its retries.
 * @returns {object} queue with add/size/onIdle methods
 */
export function createQueue(options = {}) {
  const concurrency = options.concurrency ?? 1;
  const retries = options.retries ?? 0;
  const onError = options.onError ?? (() => {});

  const pending = [];
  let running = 0;
  let nextId = 1;
  let idleResolvers = [];
  let isPaused = false;

  const runNext = () => {
    if (isPaused || running >= concurrency || pending.length === 0) {
      if (running === 0 && pending.length === 0) {
        idleResolvers.forEach((resolve) => resolve());
        idleResolvers = [];
      }
      return;
    }
    const job = pending.shift();
    running += 1;

    const attempt = (remaining) =>
      Promise.resolve()
        .then(job.fn)
        .catch((err) => {
          if (remaining > 0) return attempt(remaining - 1);
          onError(err, job.id);
        });

    attempt(retries).finally(() => {
      running -= 1;
      runNext();
    });
    runNext();
  };

  return {
    /**
     * Add a job to the queue.
     * @param {() => Promise<any>} fn - Async job function.
     * @returns {number} job id
     */
    add(fn) {
      const id = nextId++;
      pending.push({ id, fn });
      queueMicrotask(runNext);
      return id;
    },

    /** Number of jobs waiting (not counting running jobs). */
    size() {
      return pending.length;
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
