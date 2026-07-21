# relay-queue

In-memory job queue for Node.js with concurrency control and retries.

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
  onError: (err, jobId) => console.error(`job ${jobId} failed`, err),
});

queue.add(() => sendEmail("welcome", user));
queue.add(() => syncInvoice(invoiceId));

await queue.onIdle(); // resolves when everything is done
```

## API

- `createQueue(options?)` — `concurrency` (default `1`), `retries` (default `0`), `onError(err, jobId)` called when a job exhausts its retries.
- `queue.add(fn)` — enqueue an async job; returns a numeric job id.
- `queue.size()` — jobs waiting to run.
- `queue.onIdle()` — promise that resolves when the queue is drained.
- `queue.pause()` / `queue.resume()` — stop and restart job starts; running jobs always finish.
- `queue.isPaused()` — whether the queue is currently paused.
- `queue.clear()` — drop all waiting jobs without running them; returns the number dropped.

## License

MIT
