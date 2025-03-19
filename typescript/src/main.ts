type Subscriber<T> = (data: T) => void | Promise<void>;

class Event<T> {
  private subscribers = new Set<Subscriber<T>>();

  constructor(private eventBus: EventBus, public priority: "low" | "high") {}

  public subscribe(fn: Subscriber<T>) {
    this.subscribers.add(fn);
  }

  public async emit(data: T) {
    // run high priority immediately
    if (this.priority == "high") {
      return await this.run(this.subscribers, data);
    }
    // run low priority in batches of 20
    let batch = new Set<Subscriber<T>>();
    for (const subscriber of this.subscribers) {
      if (batch.add(subscriber).size >= 20) {
        await this.run(batch, data);
        batch = new Set<Subscriber<T>>(); // reset batch
      }
    }
    // run remaining batch if any
    if (batch.size) return await this.run(batch, data);
  }

  private run(subscribers: Set<Subscriber<T>>, data: T) {
    return this.eventBus.run(async () => {
      for (const subscriber of subscribers) await subscriber(data);
    });
  }
}

class EventBus {
  private ready = Promise.resolve();

  public async run(fn: () => Promise<void>) {
    // queue promises by waiting for the previous to complete execution
    this.ready = this.ready.then(fn);
    return this.ready;
  }

  public createEvent<T>(priority: "low" | "high"): Event<T> {
    return new Event(this, priority);
  }
}

const delay = (seconds: number) => {
  return new Promise<void>((res) => setTimeout(res, seconds * 1000));
};

export async function main() {
  const eventBus = new EventBus();
  const event1 = eventBus.createEvent<string>("high");
  const event2 = eventBus.createEvent<string>("low");

  const start = Date.now();
  const logger = (event: string, subscriber: string) => {
    const diff = Math.round((Date.now() - start) / 1000);
    console.log(`${diff}s:` + "\t", event + "\t", subscriber);
    return delay(1); // 1 second delay
  };

  // add event1 subscribers
  for (let i = 1; i <= 10; i++) {
    event1.subscribe((data) => logger(data, `subscriber${i}`));
  }
  // add event2 subscribers
  for (let i = 1; i <= 100; i++) {
    event2.subscribe((data) => logger(data, `subscriber${i}`));
  }

  event2.emit("event2");
  await delay(10); // 10 seconds mark
  event1.emit("event1 (t=10)");
  await delay(10); // 20 seconds mark
  event1.emit("event1 (t=20)");
  await delay(30); // 50 seconds mark
  event1.emit("event1 (t=50)");
}
main();
