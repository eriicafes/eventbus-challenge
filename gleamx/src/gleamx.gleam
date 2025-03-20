import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/otp/task
import gleam/string
import gleam/time/duration
import gleam/time/timestamp

type Priority {
  PriorityLow
  PriorityHigh
}

type Subscriber(a) =
  fn(a) -> Nil

type Event(a) {
  Event(bus: EventBus, priority: Priority, subscribers: List(Subscriber(a)))
}

type EventBus {
  EventBus(subject: Subject(#(fn() -> Nil, Subject(Nil))))
}

pub fn main() {
  let event_bus = new_event_bus()
  let event1 = new_event(event_bus, PriorityHigh)
  let event2 = new_event(event_bus, PriorityLow)

  let start = timestamp.system_time()
  let logger = fn(event, subscriber) {
    let diff =
      timestamp.system_time()
      |> timestamp.difference(start, _)
      |> duration.to_seconds
      |> float.to_precision(0)
      |> float.to_string
      |> string.drop_end(2)
    io.println(
      diff <> "s:\t " <> event <> "\t subscriber" <> int.to_string(subscriber),
    )
    // 1 second delay
    process.sleep(1000)
  }

  // add event1 subscribers
  let event1 =
    list.fold(list.range(1, 10), event1, fn(evt, i) {
      subscribe_event(evt, logger(_, i))
    })
  // add event2 subscribers
  let event2 =
    list.fold(list.range(1, 100), event2, fn(evt, i) {
      subscribe_event(evt, logger(_, i))
    })

  [
    task.async(fn() { emit_event(event2, "event2") }),
    task.async(fn() {
      // 10 seconds mark
      process.sleep(10_000)
      emit_event(event1, "event1 (t=10)")
    }),
    task.async(fn() {
      // 20 seconds mark
      process.sleep(20_000)
      emit_event(event1, "event1 (t=20)")
    }),
    task.async(fn() {
      // 50 seconds mark
      process.sleep(50_000)
      emit_event(event1, "event1 (t=50)")
    }),
  ]
  |> list.each(task.await_forever)
}

fn new_event(bus, priority) {
  Event(bus, priority, [])
}

fn subscribe_event(event: Event(a), subscriber: Subscriber(a)) {
  Event(..event, subscribers: list.append(event.subscribers, [subscriber]))
}

fn emit_event(event: Event(a), data: a) {
  case event.priority {
    PriorityHigh -> {
      use <- run(event.bus)
      event.subscribers
      |> list.each(fn(subscriber) { subscriber(data) })
    }
    PriorityLow -> {
      event.subscribers
      |> list.sized_chunk(20)
      |> list.each(fn(subscribers) {
        use <- run(event.bus)
        subscribers
        |> list.each(fn(subscriber) { subscriber(data) })
      })
    }
  }
}

fn new_event_bus() {
  let assert Ok(subject) =
    actor.start(Nil, fn(msg: #(fn() -> Nil, Subject(Nil)), _state: Nil) {
      process.send(msg.1, msg.0())
      actor.continue(Nil)
    })
  EventBus(subject)
}

fn run(bus: EventBus, f: fn() -> Nil) {
  process.call_forever(bus.subject, fn(subject) { #(f, subject) })
}
