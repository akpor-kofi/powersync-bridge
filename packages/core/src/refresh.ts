/** Collapse a burst into one read, with at most one follow-up for changes during that read. */
export class CoalescedRefresh {
  private revision = 0;
  private lifecycle = 0;
  private scheduled = false;
  private running = false;

  constructor(
    private readonly read: (isCurrent: () => boolean) => Promise<void>,
    private readonly settled: () => void,
  ) {}

  get pending(): boolean {
    return this.scheduled || this.running;
  }

  request(): void {
    this.revision += 1;
    if (this.pending) return;
    this.scheduled = true;
    const lifecycle = this.lifecycle;
    queueMicrotask(() => {
      if (lifecycle !== this.lifecycle) return;
      this.scheduled = false;
      void this.drain(lifecycle);
    });
  }

  /** Fence queued/late work without retaining results or preventing a later resume. */
  cancel(): void {
    this.lifecycle += 1;
    this.scheduled = false;
    this.running = false;
  }

  private async drain(lifecycle: number): Promise<void> {
    this.running = true;
    let revision: number;
    try {
      do {
        revision = this.revision;
        await this.read(() => lifecycle === this.lifecycle && revision === this.revision);
      } while (lifecycle === this.lifecycle && revision !== this.revision);
    } finally {
      if (lifecycle === this.lifecycle) {
        this.running = false;
        this.settled();
      }
    }
  }
}

interface QueuedRead {
  run(): void;
  cancel(): void;
}

/** Checkpoint maintenance has one shared slot; first reads and pagination bypass it. */
export class BackgroundReads {
  private running = false;
  private readonly queue = new Set<QueuedRead>();

  run<T>(read: () => Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
      const entry: QueuedRead = {
        cancel: () => {
          if (!this.queue.delete(entry)) return;
          signal.removeEventListener('abort', entry.cancel);
          reject(signal.reason);
        },
        run: () => {
          signal.removeEventListener('abort', entry.cancel);
          this.running = true;
          void Promise.resolve()
            .then(read)
            .then(resolve, reject)
            .finally(() => {
              this.running = false;
              this.drain();
            });
        },
      };
      signal.addEventListener('abort', entry.cancel, { once: true });
      this.queue.add(entry);
      this.drain();
    });
  }

  private drain(): void {
    if (this.running) return;
    const next = this.queue.values().next().value;
    if (!next) return;
    this.queue.delete(next);
    next.run();
  }
}
