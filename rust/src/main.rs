use std::{pin::Pin, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Duration, Instant},
};

enum Priority {
    Low,
    High,
}

type FutureCallback = Pin<Box<dyn Future<Output = ()> + Send>>;

struct EventBus {
    runner_tx: mpsc::Sender<(FutureCallback, oneshot::Sender<()>)>,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel::<(FutureCallback, oneshot::Sender<()>)>(1);
        // execute notifiers serially in receiving task
        tokio::spawn(async move {
            while let Some((future, done_tx)) = rx.recv().await {
                future.await;
                let _ = done_tx.send(());
            }
        });
        Self { runner_tx: tx }
    }

    async fn run<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.runner_tx.send((Box::pin(future), done_tx)).await;
        let _ = done_rx.await;
    }

    pub fn create_event<'a, T: Copy + Send + Sync>(
        self: &Arc<Self>,
        priority: Priority,
    ) -> Event<T> {
        Event {
            bus: self.clone(),
            priority,
            subscribers: Vec::new(),
        }
    }
}

type Subscriber<T> = Box<dyn (Fn(T) -> FutureCallback) + Send + Sync>;

struct Event<T: Send + Sync> {
    bus: Arc<EventBus>,
    priority: Priority,
    subscribers: Vec<Subscriber<T>>,
}

impl<T: Clone + Send + Sync> Event<T> {
    pub fn subscribe<F, Fut>(&mut self, func: F)
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.subscribers
            .push(Box::new(move |data| Box::pin(func(data))));
    }

    pub async fn emit(&self, data: T) {
        match self.priority {
            Priority::High => {
                let notifiers: Vec<_> = self
                    .subscribers
                    .iter()
                    .map(move |subscriber| subscriber(data.clone()))
                    .collect();
                self.bus
                    .run(async move {
                        for notifier in notifiers {
                            notifier.await
                        }
                    })
                    .await
            }
            Priority::Low => {
                for batch in self.subscribers.chunks(20) {
                    let data = data.clone();
                    let notifiers: Vec<_> = batch
                        .iter()
                        .map(move |subscriber| subscriber(data.clone()))
                        .collect();
                    self.bus
                        .run(async move {
                            for notifier in notifiers {
                                notifier.await
                            }
                        })
                        .await
                }
            }
        }
    }
}

async fn logger(start: Instant, event: &str, subscriber: &str) {
    let diff = start.elapsed().as_secs();
    println!("{diff}s:\t {event}\t {subscriber}");
    time::sleep(Duration::from_secs(1)).await; // 1 second delay
}

#[tokio::main]
async fn main() {
    let event_bus = Arc::new(EventBus::new());
    let mut event1 = event_bus.create_event(Priority::High);
    let mut event2 = event_bus.create_event(Priority::Low);

    let start = time::Instant::now();
    // add event1 subscribers
    for i in 0..10 {
        let start = start.clone();
        event1.subscribe(move |data| async move {
            logger(start, data, format!("subscriber{}", i + 1).as_str()).await
        });
    }
    // add event2 subscribers
    for i in 0..100 {
        let start = start.clone();
        event2.subscribe(move |data| async move {
            logger(start, data, format!("subscriber{}", i + 1).as_str()).await
        });
    }

    let (event1, event2) = (Arc::new(event1), Arc::new(event2));
    let _ = tokio::join!(
        tokio::spawn(async move {
            event2.emit("event2").await;
        }),
        {
            let event1 = event1.clone();
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(10)).await; // 10 seconds mark
                event1.emit("event1 (t=10)").await;
            })
        },
        {
            let event1 = event1.clone();
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(20)).await; // 20 seconds mark
                event1.emit("event1 (t=20)").await;
            })
        },
        {
            let event1 = event1.clone();
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(50)).await; // 50 seconds mark
                event1.emit("event1 (t=50)").await;
            })
        }
    );
}
