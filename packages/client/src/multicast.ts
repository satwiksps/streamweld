import { StreamBufferLimitError } from "./errors.js";
import type { StreamSequence } from "./types.js";

interface Deferred {
  readonly promise: Promise<void>;
  resolve(): void;
}

interface RecordValue<T> {
  readonly index: number;
  readonly value: T;
  readonly cumulativeBytes: number;
}

interface Subscriber {
  index: number;
  cumulativeBytes: number;
  waiting: Deferred | null;
  failure: unknown;
  closed: boolean;
}

/** One producer with independent cursors for every active async iterator. */
export class ReplayMulticast<T> {
  readonly #maxEvents: number;
  readonly #maxBytes: number;
  readonly #sizeOf: (value: T) => number;
  readonly #sequenceOf: (value: T) => StreamSequence;
  readonly #subscribers = new Set<Subscriber>();
  #records: RecordValue<T>[] = [];
  #baseIndex = 0;
  #nextIndex = 0;
  #totalBytes = 0;
  #settled = false;
  #failure: unknown;
  #everSubscribed = false;
  #lateReplayFailure: StreamBufferLimitError | null = null;
  #lastSequence: StreamSequence = "0";

  constructor(options: {
    maxEvents: number;
    maxBytes: number;
    sizeOf(value: T): number;
    sequenceOf(value: T): StreamSequence;
  }) {
    this.#maxEvents = options.maxEvents;
    this.#maxBytes = options.maxBytes;
    this.#sizeOf = options.sizeOf;
    this.#sequenceOf = options.sequenceOf;
  }

  iterable(): AsyncIterable<T> {
    return { [Symbol.asyncIterator]: () => this.subscribe() };
  }

  /** Reserve an independent cursor before the producer publishes. */
  subscribe(): AsyncIterator<T> {
    return this.#createSubscriber();
  }

  publish(value: T): void {
    if (this.#settled) return;
    const bytes = Math.max(0, this.#sizeOf(value));
    this.#lastSequence = this.#sequenceOf(value);
    this.#totalBytes += bytes;
    this.#records.push({
      index: this.#nextIndex,
      value,
      cumulativeBytes: this.#totalBytes,
    });
    this.#nextIndex += 1;

    for (const subscriber of this.#subscribers) {
      const queuedEvents = this.#nextIndex - subscriber.index;
      const queuedBytes = this.#totalBytes - subscriber.cumulativeBytes;
      if (queuedEvents > this.#maxEvents || queuedBytes > this.#maxBytes) {
        subscriber.failure = new StreamBufferLimitError(this.#sequenceOf(value));
        subscriber.closed = true;
        this.#wake(subscriber);
      } else {
        this.#wake(subscriber);
      }
    }
    this.#removeClosed();
    this.#compact();
  }

  close(): void {
    if (this.#settled) return;
    this.#settled = true;
    for (const subscriber of this.#subscribers) this.#wake(subscriber);
  }

  fail(error: unknown): void {
    if (this.#settled) return;
    this.#failure = error;
    this.#settled = true;
    for (const subscriber of this.#subscribers) this.#wake(subscriber);
  }

  #createSubscriber(): AsyncIterator<T> {
    const firstRecord = this.#records[0];
    const subscriber: Subscriber = {
      index: this.#baseIndex,
      cumulativeBytes: firstRecord === undefined
        ? this.#totalBytes
        : firstRecord.cumulativeBytes - this.#sizeOf(firstRecord.value),
      waiting: null,
      failure: undefined,
      closed: false,
    };
    if (this.#lateReplayFailure !== null) {
      subscriber.index = this.#nextIndex;
      subscriber.cumulativeBytes = this.#totalBytes;
      subscriber.failure = this.#lateReplayFailure;
    }
    this.#everSubscribed = true;
    this.#subscribers.add(subscriber);

    return {
      next: async (): Promise<IteratorResult<T>> => {
        while (true) {
          if (subscriber.failure !== undefined) {
            this.#unsubscribe(subscriber);
            throw subscriber.failure;
          }
          if (subscriber.index < this.#baseIndex) {
            subscriber.index = this.#baseIndex;
            const first = this.#records[0];
            subscriber.cumulativeBytes = first === undefined
              ? this.#totalBytes
              : first.cumulativeBytes - this.#sizeOf(first.value);
          }
          if (subscriber.index < this.#nextIndex) {
            const record = this.#records[subscriber.index - this.#baseIndex];
            if (record === undefined) continue;
            subscriber.index += 1;
            subscriber.cumulativeBytes = record.cumulativeBytes;
            this.#compact();
            return { done: false, value: record.value };
          }
          if (this.#settled) {
            this.#unsubscribe(subscriber);
            if (this.#failure !== undefined) throw this.#failure;
            return { done: true, value: undefined };
          }
          subscriber.waiting ??= deferred();
          await subscriber.waiting.promise;
        }
      },
      return: async (): Promise<IteratorResult<T>> => {
        this.#unsubscribe(subscriber);
        return { done: true, value: undefined };
      },
      throw: async (error?: unknown): Promise<IteratorResult<T>> => {
        this.#unsubscribe(subscriber);
        throw error;
      },
    };
  }

  #wake(subscriber: Subscriber): void {
    const waiting = subscriber.waiting;
    subscriber.waiting = null;
    waiting?.resolve();
  }

  #unsubscribe(subscriber: Subscriber): void {
    subscriber.closed = true;
    this.#subscribers.delete(subscriber);
    this.#wake(subscriber);
    this.#compact();
  }

  #removeClosed(): void {
    for (const subscriber of this.#subscribers) {
      if (subscriber.closed) this.#subscribers.delete(subscriber);
    }
  }

  #compact(): void {
    if (this.#records.length === 0) return;
    let retainFrom: number;
    if (this.#subscribers.size > 0) {
      retainFrom = Math.min(...[...this.#subscribers].map((subscriber) => subscriber.index));
    } else if (!this.#everSubscribed) {
      retainFrom = this.#baseIndex;
      while (
        this.#nextIndex - retainFrom > this.#maxEvents ||
        this.#totalBytes - this.#cumulativeBefore(retainFrom) > this.#maxBytes
      ) {
        retainFrom += 1;
      }
    } else {
      retainFrom = this.#nextIndex;
    }
    const remove = retainFrom - this.#baseIndex;
    if (remove <= 0) return;
    // A future iterator must never mistake a truncated local replay for the
    // beginning of the stream. Existing active iterators keep their own safe
    // cursors; only a subsequently attached iterator receives this failure.
    this.#lateReplayFailure ??= new StreamBufferLimitError(this.#lastSequence);
    this.#records.splice(0, remove);
    this.#baseIndex = retainFrom;
  }

  #cumulativeBefore(index: number): number {
    if (index <= this.#baseIndex) {
      const first = this.#records[0];
      return first === undefined ? this.#totalBytes : first.cumulativeBytes - this.#sizeOf(first.value);
    }
    const previous = this.#records[index - this.#baseIndex - 1];
    return previous?.cumulativeBytes ?? this.#totalBytes;
  }
}

function deferred(): Deferred {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
