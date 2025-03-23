// The Swift Programming Language
// https://docs.swift.org/swift-book

import Foundation

typealias Subscriber<T> = @Sendable (T) async -> Void

enum Priority {
    case low
    case high
}

actor Event<T: Sendable> {
    private var bus: EventBus
    private var priority: Priority
    private var subscribers: [Subscriber<T>] = []

    init(bus: EventBus, priority: Priority) {
        self.bus = bus
        self.priority = priority
    }

    func subscribe(_ fn: @escaping Subscriber<T>) {
        subscribers.append(fn)
    }

    func emit(_ data: T) async {
        switch priority {
        case .high:
            let subscribers = self.subscribers
            await bus.run {
                for subscriber in subscribers {
                    await subscriber(data)
                }
            }
        case .low:
            for i in stride(from: 0, through: subscribers.count, by: 20) {
                let batch = subscribers[i..<Swift.min(i + 20, subscribers.count)]
                await bus.run {
                    for subscriber in batch {
                        await subscriber(data)
                    }
                }
            }
        }
    }
}

actor EventBus {
    var task: Task<Void, Never>?

    func run(fn: @Sendable @escaping () async -> Void) async {
        if let task {
            self.task = Task {
                await task.value
                await fn()
            }
            await self.task?.value
        } else {
            self.task = Task {
                await fn()
            }
            await self.task?.value
        }
    }
}

func logger(start: Date, event: String, subscriber: String) async throws {
    let diff = Int(abs(start.timeIntervalSinceNow))
    print("\(diff)s:\t \(event)\t \(subscriber)")
    try await Task.sleep(for: .seconds(1), tolerance: .zero)
}

let eventbus = EventBus()
var event1 = Event<String>(bus: eventbus, priority: .high)
var event2 = Event<String>(bus: eventbus, priority: .low)

let start = Date()
for i in 1...10 {
    await event1.subscribe { data in
        try? await logger(start: start, event: data, subscriber: "subscriber\(i)")
    }
}
for i in 1...100 {
    await event2.subscribe { data in
        try? await logger(start: start, event: data, subscriber: "subscriber\(i)")
    }
}

async let e2: Void = event2.emit("event2")
try await Task.sleep(for: .seconds(10))  // 10 seconds mark
async let e11: Void = event1.emit("event1 (t=10)")
try await Task.sleep(for: .seconds(10))  // 20 seconds mark
async let e12: Void = event1.emit("event1 (t=20)")
try await Task.sleep(for: .seconds(30))  // 50 seconds mark
async let e13: Void = event1.emit("event1 (t=50)")
let _ = await [e2, e11, e12, e13]
