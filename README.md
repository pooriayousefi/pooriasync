
# PooriAsync

A header-only, dependency-free C++23 async runtime combining coroutine primitives, a work-stealing CPU thread pool, an epoll/kqueue I/O reactor, and RAII process management — all in four headers with zero external dependencies.

```
asyncore.hpp           → Coroutine primitives (AsyncTask, DetachedTask, ...)
io_thread_pool.hpp     → I/O-bound pool (epoll/kqueue reactor, AsyncSocket, AsyncPipe)
cpu_thread_pool.hpp    → CPU-bound pool (Chase-Lev work-stealing, TaskGroup)
process.hpp            → Process management (fork/exec/waitpid, RAII, no zombies)
```

**Unix-only (Linux / Mac / BSD). Windows users: use WSL2.**

---

## Table of contents

- [PooriAsync](#pooriasync)
  - [Table of contents](#table-of-contents)
  - [Why PooriAsync?](#why-pooriasync)
  - [Which Thread Pool Should I Use?](#which-thread-pool-should-i-use)
    - [Decision Flowchart](#decision-flowchart)
  - [Architecture](#architecture)
  - [Features](#features)
    - [`asyncore.hpp` — Coroutine Foundation](#asyncorehpp--coroutine-foundation)
    - [`io_thread_pool.hpp` — I/O-Bound Pool](#io_thread_poolhpp--io-bound-pool)
    - [`cpu_thread_pool.hpp` — CPU-Bound Pool](#cpu_thread_poolhpp--cpu-bound-pool)
    - [`process.hpp` — Process Management](#processhpp--process-management)
  - [Requirements](#requirements)
  - [Project Structure](#project-structure)
  - [Building](#building)
  - [Core Components](#core-components)
  - [Usage](#usage)
    - [Coroutine Primitives (asyncore.hpp)](#coroutine-primitives-asyncorehpp)
    - [I/O Thread Pool (io\_thread\_pool.hpp)](#io-thread-pool-io_thread_poolhpp)
    - [CPU Thread Pool (cpu\_thread\_pool.hpp)](#cpu-thread-pool-cpu_thread_poolhpp)
    - [Process Management (process.hpp)](#process-management-processhpp)
    - [Combining I/O + CPU + Process (Real-World)](#combining-io--cpu--process-real-world)
  - [Comparison](#comparison)
  - [Limitations and Gotchas](#limitations-and-gotchas)
  - [License](#license)

---

## Why PooriAsync?

Most C++ async libraries force you into a single model — either `boost::asio` (I/O-centric, callback-heavy) or a raw thread pool (CPU-centric, no reactor). PooriAsync gives you **both** under one roof, with native C++23 coroutines throughout:

| Layer | Typical Library | PooriAsync |
|-------|----------------|------------|
| Coroutines | `boost::asio::awaitable` (tied to asio) | `asyncore.hpp` — standalone `AsyncTask<T>`, `DetachedTask`, `FireAndForget` |
| I/O reactor | `boost::asio` io_context (~500KB) | `io_thread_pool.hpp` — epoll/kqueue, ~500 LOC, zero deps |
| CPU scheduling | `boost::asio::thread_pool` (no work-stealing) | `cpu_thread_pool.hpp` — Chase-Lev work-stealing deque |
| Process mgmt | `boost::process` (heavy dep) | `process.hpp` — RAII, no zombies, noexcept terminate |
| HTTP client | `libcurl` or `cpp-httplib` | `AsyncHTTPClient` (in poorimcp, built on `AsyncSocket`) |
| Structured concurrency | None (C++26 proposal) | `TaskGroup` — spawn + wait + exception propagation |
| Cancellation | None (ad-hoc `atomic<bool>`) | `CancellationToken` — thread-safe, `throw_if_cancelled()` |

**No `boost`. No `asio`. No `libuv`. No `libcurl`. No exceptions for control flow.**

---

## Which Thread Pool Should I Use?

| Scenario | Use | Why |
|----------|-----|-----|
| **Network server / client** | `io_bound::ThreadPool` | epoll/kqueue reactor detects socket readiness; coroutines suspend until data arrives; no CPU wasted spinning |
| **HTTP API calls** | `io_bound::ThreadPool` | `AsyncHTTPClient` uses `AsyncSocket` — non-blocking connect/send/recv with reactor-driven resumption |
| **IPC / stdio pipes** | `io_bound::ThreadPool` | `AsyncPipe` registers pipe fds with epoll/kqueue; child process output streams without blocking |
| **Number crunching** | `cpu_bound::ThreadPool` | Chase-Lev work-stealing deque keeps CPU cache hot; no reactor overhead; `submit()` returns `std::future` |
| **Data processing pipelines** | `cpu_bound::ThreadPool` | `TaskGroup` for structured concurrency — spawn parallel stages, `co_await group.wait()` |
| **Mixed I/O + CPU** | **Both** | I/O pool handles network; CPU pool handles computation; communicate via `std::future` or shared state |
| **Spawning child processes** | `process::Process` | RAII wrapper — no zombies, noexcept `terminate()`, idempotent `wait()` |
| **Fire-and-forget background tasks** | Either pool's `spawn()` | `DetachedTask` auto-destroys on completion; no leaks, no manual cleanup |

### Decision Flowchart

```
Is the work I/O-bound (network, pipes, files)?
├── Yes → io_bound::ThreadPool
│         (epoll/kqueue reactor, AsyncSocket, AsyncPipe)
│
└── No → Is it CPU-bound (computation, algorithms)?
    ├── Yes → cpu_bound::ThreadPool
    │         (Chase-Lev work-stealing, TaskGroup)
    │
    └── No → Do you need to spawn processes?
        └── Yes → process::Process
                  (fork/exec/waitpid, RAII)
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      asyncore.hpp                            │
│   AsyncTask<T>  DetachedTask  FireAndForget  AsyncGenerator  │
│   CancellationToken  MoveOnlyFunction  sync_wait()           │
└────────────────────────┬─────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
          ▼                             ▼
┌──────────────────────┐   ┌──────────────────────────┐
│  io_thread_pool.hpp  │   │   cpu_thread_pool.hpp    │
│                      │   │                          │
│  NetworkReactor      │   │  ChaseLevDeque           │
│   (epoll / kqueue)   │   │   (lock-free work-steal) │
│  AsyncSocket         │   │  ThreadPool              │
│   (TCP send/recv)    │   │   (run / spawn / submit) │
│  AsyncPipe           │   │  TaskGroup               │
│   (IPC send/recv)    │   │   (structured conc.)     │
│  ThreadPool          │   │                          │
│   (run / submit)     │   │  No reactor — pure CPU   │
│                      │   │  Work-stealing scheduler │
│  Reactor-driven      │   │                          │
│  event loop          │   │                          │
└──────────────────────┘   └──────────────────────────┘

┌──────────────────────┐
│     process.hpp      │
│                      │
│  Process (RAII)      │
│   fork + execvp      │
│   waitpid            │
│   kill(SIGTERM)      │
│   close_stdin()      │
│                      │
│  FileDescriptor      │
│   (RAII fd wrapper)  │
│                      │
│  No zombies          │
│  Noexcept terminate  │
│  Idempotent wait     │
└──────────────────────┘
```

---

## Features

### `asyncore.hpp` — Coroutine Foundation

- **`AsyncTask<T>`** — lazy coroutine returning `T`; awaitable with symmetric transfer
- **`AsyncTask<void>`** — void specialization
- **`AsyncGenerator<T>`** — lazy coroutine yielding multiple values; iterable
- **`FireAndForget`** — eager coroutine (starts immediately); self-managed lifetime
- **`DetachedTask`** — lazy coroutine (starts suspended); auto-destroys frame on completion — no leaks, no UB
- **`SyncWaitTask<T>`** + **`sync_wait()`** — block calling thread until coroutine completes
- **`CancellationToken`** — thread-safe, one-way cancel flag; `cancel()`, `is_cancelled()`, `throw_if_cancelled()`
- **`CancelledException`** — thrown by `throw_if_cancelled()`
- **`MoveOnlyFunction`** — type-erased, move-only callable (replaces `std::function` for move-only types)

### `io_thread_pool.hpp` — I/O-Bound Pool

- **`NetworkReactor`** — per-thread event loop: `epoll` (Linux), `kqueue` (Mac/BSD); 100ms timeout fallback
- **`AsyncSocket`** — non-blocking TCP: `co_await async_connect()`, `co_await send()`, `co_await recv()`
- **`AsyncPipe`** — non-blocking IPC: `co_await send()`, `co_await recv()`; integrates with `process.hpp`
- **`ThreadPool`** — round-robin worker pool, each with its own `NetworkReactor`; `submit()`, `run()`, `schedule()`
- **`std::expected<T, std::error_code>`** — all I/O returns expected, never throws
- **`std::span<std::byte>`** — bounds-safe, zero-copy buffers
- **Thread-local reactor** — `NetworkReactor::current` set by `run()`; `AsyncSocket`/`AsyncPipe` auto-detect

### `cpu_thread_pool.hpp` — CPU-Bound Pool

- **`ChaseLevDeque`** — lock-free work-stealing deque; workers pop LIFO (cache locality), stealers steal FIFO
- **`ThreadPool`** — work-stealing scheduler with local queues + global fallback
  - `run(AsyncTask<T>)` — returns `std::future<T>`
  - `spawn(AsyncTask<T>)` — fire-and-forget
  - `submit(F, Args...)` — plain callable, returns `std::future<R>`
  - `schedule()` — `co_await` to yield to the pool (pushes to global queue for thread switch)
  - `wait()` — block until all tasks complete
- **`TaskGroup`** — structured concurrency: spawn multiple tasks, `co_await group.wait()`, first exception rethrown
- **`DetachedTask`** — all coroutines use lazy start + auto-destroy; no dangling handles

### `process.hpp` — Process Management

- **`Process`** — RAII cross-platform process lifecycle
  - `start(executable, args)` — `fork()` + `execvp()` (POSIX)
  - `wait()` — `waitpid()`, returns exit code; idempotent (returns cached code if already reaped)
  - `terminate()` — `SIGTERM` + `waitpid()`; `noexcept` — safe in destructors
  - `running()` — `waitpid(WNOHANG)`; reaps zombies and updates state (no `ECHILD` on later `wait()`)
  - `close_stdin()` — signals EOF to child
  - `exit_code()` — last known exit code
- **`FileDescriptor`** — RAII wrapper for POSIX file descriptors; move-only
- **No zombies, no orphans** — destructor calls `terminate()` + `wait()` if child still running
- **`initializer_list` convenience** — `p.start("cat", {"hello"})`

---

## Requirements

- **C++23** (`std::expected`, `std::coroutine`, `std::span`, `std::binary_semaphore`, `std::format`)
- Clang 16+ (macOS/Linux), GCC 13+ (Linux)
- **Unix only** — Linux (epoll), Mac/BSD (kqueue)
- No external dependencies

## Project Structure

```text
pooriasync/
├── bin/
├── include/
│   ├── asyncore.hpp
│   ├── io_thread_pool.hpp
│   ├── cpu_thread_pool.hpp
│   └── process.hpp
├── src/
│   ├── io_thread_pool_test.cpp
│   ├── cpu_thread_pool_test.cpp
│   └── process_test.cpp
└── README.md
```

## Building

**Mac/Linux:**
```bash
mkdir -p bin

# I/O thread pool tests
clang++ -std=c++23 -O3 -I include src/io_thread_pool_test.cpp -o bin/io_test
./bin/io_test

# CPU thread pool tests
clang++ -std=c++23 -O3 -I include src/cpu_thread_pool_test.cpp -o bin/cpu_test
./bin/cpu_test

# Process management tests
clang++ -std=c++23 -O3 -I include src/process_test.cpp -o bin/process_test
./bin/process_test
```

---

## Core Components

| Component | Header | Namespace | Description |
|-----------|--------|-----------|-------------|
| `AsyncTask<T>` | `asyncore.hpp` | `core` | Lazy coroutine returning `T` |
| `AsyncGenerator<T>` | `asyncore.hpp` | `core` | Lazy coroutine yielding multiple `T` |
| `FireAndForget` | `asyncore.hpp` | `core` | Eager, self-managed coroutine |
| `DetachedTask` | `asyncore.hpp` | `core` | Lazy, auto-destroying coroutine |
| `CancellationToken` | `asyncore.hpp` | `core` | Thread-safe cancel flag |
| `MoveOnlyFunction` | `asyncore.hpp` | `core` | Type-erased move-only callable |
| `sync_wait()` | `asyncore.hpp` | `core` | Block until coroutine completes |
| `NetworkReactor` | `io_thread_pool.hpp` | `io_bound` | Per-thread event loop (epoll/kqueue) |
| `AsyncSocket` | `io_thread_pool.hpp` | `io_bound` | Non-blocking TCP with `co_await` |
| `AsyncPipe` | `io_thread_pool.hpp` | `io_bound` | Non-blocking IPC with `co_await` |
| `ThreadPool` (I/O) | `io_thread_pool.hpp` | `io_bound` | Round-robin reactor pool |
| `ChaseLevDeque` | `cpu_thread_pool.hpp` | `cpu_bound` | Lock-free work-stealing deque |
| `ThreadPool` (CPU) | `cpu_thread_pool.hpp` | `cpu_bound` | Work-stealing scheduler |
| `TaskGroup` | `cpu_thread_pool.hpp` | `cpu_bound` | Structured concurrency |
| `Process` | `process.hpp` | `process` | RAII process lifecycle |
| `FileDescriptor` | `process.hpp` | `process` | RAII fd wrapper |

---

## Usage

### Coroutine Primitives (asyncore.hpp)

```cpp
#include "asyncore.hpp"

using namespace pooriayousefi::core;

// AsyncTask<T> — lazy coroutine, returns a value
AsyncTask<int> compute() {
    co_return 42;
}

// AsyncGenerator<T> — yields multiple values
AsyncGenerator<int> range(int n) {
    for (int i = 1; i <= n; ++i) {
        co_yield i * 10;
    }
}

// FireAndForget — eager start, self-managed
FireAndForget background_task() {
    // starts immediately when created
    co_return;
}

// sync_wait — block until done (use only on non-reactor threads)
int result = sync_wait(compute());
```

### I/O Thread Pool (io_thread_pool.hpp)

```cpp
#include "io_thread_pool.hpp"

using namespace pooriayousefi::io_bound;
using namespace pooriayousefi::core;

// TCP server
ThreadPool pool{4};

auto server_task = [&]() -> AsyncTask<void> {
    // AsyncSocket auto-detects the thread-local reactor
    AsyncSocket sock;
    auto connect_result = co_await sock.async_connect("example.com", 80);
    if (!connect_result) {
        co_return;
    }

    std::string request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    co_await sock.send(std::as_bytes(std::span{request}));

    std::byte buf[4096];
    auto n = co_await sock.recv(buf);
    if (n && *n > 0) {
        std::string response(reinterpret_cast<const char*>(buf), *n);
    }
};

auto fut = pool.run(server_task());
fut.get();
```

### CPU Thread Pool (cpu_thread_pool.hpp)

```cpp
#include "cpu_thread_pool.hpp"

using namespace pooriayousefi::cpu_bound;
using namespace pooriayousefi::core;

// Work-stealing pool for CPU-bound computation
ThreadPool pool{4};

// Method 1: run a coroutine
auto compute = [&pool]() -> AsyncTask<int> {
    co_await pool.schedule();  // yield to pool (may switch threads)
    int sum = 0;
    for (int i = 1; i <= 1000; ++i) {
        sum += i;
    }
    co_return sum;
};

std::future<int> fut = pool.run(compute());
int result = fut.get();

// Method 2: submit a plain callable
auto fut2 = pool.submit([](int a, int b) {
    return a * b;
}, 6, 7);

// Method 3: structured concurrency
TaskGroup group{pool};
std::atomic<int> total{0};

for (int i = 1; i <= 10; ++i) {
    group.spawn([&pool, &total, i]() -> AsyncTask<void> {
        co_await pool.schedule();
        total.fetch_add(i, std::memory_order_relaxed);
    });
}

// Wait for all — rethrows first exception if any
auto waiter = [&group]() -> AsyncTask<void> {
    co_await group.wait();
};
pool.run(waiter()).get();
```

### Process Management (process.hpp)

```cpp
#include "process.hpp"

using namespace pooriayousefi::process;

// Spawn a child process with piped stdin/stdout
Process p;
p.start("/bin/cat", {});

// Write to child's stdin
::write(p.get_stdin_write(), "hello\n", 6);
p.close_stdin();  // signal EOF so cat exits

// Read from child's stdout
char buf[256];
ssize_t n = ::read(p.get_stdout_read(), buf, sizeof(buf));

// Wait for exit
int code = p.wait();
// code == 0

// Or terminate if still running
// p.terminate();  // noexcept — safe in destructors
```

### Combining I/O + CPU + Process (Real-World)

```cpp
#include "io_thread_pool.hpp"
#include "cpu_thread_pool.hpp"
#include "process.hpp"

using namespace pooriayousefi;

// I/O pool for network, CPU pool for computation
io_bound::ThreadPool io_pool{2};  // fewer threads — I/O is mostly waiting
cpu_bound::ThreadPool cpu_pool{4}; // more threads — CPU-bound work

// Spawn an MCP server as a subprocess
process::Process mcp_server;
mcp_server.start("npx", {"-y", "@modelcontextprotocol/server-filesystem", "/tmp"});

// Use AsyncPipe (on io_pool's reactor) to communicate with the subprocess
// Use cpu_pool to run heavy computation on the data received
```

---

## Comparison

| Feature | PooriAsync | boost::asio | libuv | tokio (Rust) |
|---------|-----------|-------------|-------|-------------|
| **Dependencies** | Zero | Boost (~20MB) | libuv (~1MB) | Rust stdlib + tokio |
| **Coroutines** | C++23 native | `co_await` (asio-specific) | Callbacks | `async/await` |
| **I/O reactor** | epoll/kqueue | epoll/kqueue/io_uring | epoll/kqueue | epoll/kqueue/io_uring |
| **CPU work-stealing** | Chase-Lev deque | No (thread_pool only) | No | Yes (tokio + rayon) |
| **Structured concurrency** | `TaskGroup` | No | No | `JoinSet` |
| **Process mgmt** | RAII, no zombies | `boost::process` (heavy) | `uv_spawn` | `std::process::Command` |
| **Cancellation** | `CancellationToken` | No | `uv_cancel_t` | `CancellationToken` |
| **Error handling** | `std::expected` | Exceptions / `error_code` | int return codes | `Result<T, E>` |
| **Header-only** | Yes | No (compiled lib) | No | No |
| **Binary size** | Minimal | ~500KB+ | ~100KB+ | Large |
| **Compile time** | Fast | Slow (template explosion) | Fast | Fast |
| **Platform** | Unix only | Cross-platform | Cross-platform | Cross-platform |

---

## Limitations and Gotchas

1. **Unix only.** Linux (epoll) and Mac/BSD (kqueue). No Windows native support — use WSL2.
2. **No TLS/SSL.** `AsyncSocket` is plaintext TCP. For TLS, layer OpenSSL on top.
3. **No chunked HTTP.** `AsyncHTTPClient` (in `poorimcp.hpp`) uses `Connection: close`. For keep-alive or chunked encoding, extend the client.
4. **`sync_wait` deadlocks.** Calling `sync_wait` inside a reactor thread blocks that thread. Use `ThreadPool::run()` instead.
5. **Thread-local reactor.** `AsyncSocket` and `AsyncPipe` rely on `NetworkReactor::current`. You cannot use them on a thread not running `NetworkReactor::run()`.
6. **Coroutine frame heap allocation.** Each `co_await` creates a frame on the heap. For ultra-low-latency, add a pooled allocator.
7. **`wait()` can deadlock.** Calling `cpu_bound::ThreadPool::wait()` from a worker thread blocks that worker. Call only from non-worker threads.
8. **Object key order is unspecified.** JSON objects use `std::unordered_map` — key order varies between runs.
9. **`TaskGroup` stores only first exception.** Subsequent exceptions are swallowed.
10. **`ChaseLevDeque` has fixed capacity (1024).** Overflow falls back to the global queue — no data loss, but reduced locality.
11. **`Process` destructor terminates running children.** If the `Process` object is destroyed while the child is still running, it sends `SIGTERM` and reaps. Call `wait()` first for graceful shutdown.
12. **No `stderr` pipe.** `Process` pipes stdin and stdout only. stderr is inherited from the parent.

---

## License

Apache License 2.0

---

**Author:** Pooria Yousefi

---