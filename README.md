# relay-queue

In-memory job queue for Node.js with concurrency control, priorities,
per-job timeouts, and retries with exponential backoff. Zero dependencies.

## Install

```bash
npm install relay-queue
```

## Usage

```js
import { createQueue } from "relay-queue";

const queue = createQueue({
  concurrency: 2,
  retries: 3,
  retryDelayMs: 250,   // 250ms, then 500ms, then 1s (backoffFactor: 2)
  timeoutMs: 10_000,   // per attempt
  onError: (err, jobId) => console.error(`job ${jobId} failed`, err),
});

queue.add(() => sendEmail("welcome", user));
queue.add(() => chargeCard(order), { priority: 10, timeoutMs: 5_000 });

await queue.onIdle(); // resolves when everything is done
queue.stats(); // { added, completed, failed, retried, timedOut, running, pending }
```

## API

### Creating a queue

- `createQueue(options?)`:
  - `concurrency` (default `1`) — max jobs running at once.
  - `retries` (default `0`) — retry attempts per job after the first failure.
  - `timeoutMs` — per-attempt time limit; a timed-out attempt counts as a failure and consumes a retry.
  - `retryDelayMs` (default `0`) — delay before the first retry.
  - `backoffFactor` (default `2`) — multiplier applied to the delay on each subsequent retry.
  - `onError(err, jobId)` — called when a job exhausts its retries. Timeouts surface as `TimeoutError`.

### Adding work

- `queue.add(fn, options?)` — enqueue an async job; returns a numeric job id. `options.priority` (default `0`, higher runs first, FIFO within a priority) and `options.timeoutMs` override per job.
- `queue.addAll(fns)` — enqueue several jobs at once; returns their ids in input order.

### Observing

- `queue.size()` — jobs waiting to run.
- `queue.running()` — jobs currently in flight (a job waiting on a retry delay still counts).
- `queue.stats()` — lifetime counters plus current state: `{ added, completed, failed, retried, timedOut, running, pending }`.
- `queue.onIdle()` — promise that resolves when the queue is fully drained, including retry delays.

### Controlling

- `queue.pause()` / `queue.resume()` — stop and restart job starts; running jobs always finish.
- `queue.isPaused()` — whether the queue is currently paused.
- `queue.clear()` — drop all waiting jobs without running them; returns the number dropped.

## Testing

```bash
npm test
```

## License

MIT
