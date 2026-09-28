
# PooriAsync

A header-only, dependency-free C++23 async runtime combining coroutine primitives, a work-stealing CPU thread pool, a cross-platform I/O thread pool with coroutine bridging, and RAII process management — all in four headers with zero external dependencies.

```
asyncore.hpp           → Coroutine primitives (AsyncTask, DetachedTask, CancellationToken, ...)
io_thread_pool.hpp     → Cross-platform I/O pool (Standard threads + run_blocking() bridge)
cpu_thread_pool.hpp    → CPU-bound pool (Chase-Lev work-stealing, TaskGroup)
process.hpp            → Process management (fork/exec, CreateProcessA, RAII, no zombies)
```

**Cross-Platform (Mac / Linux / Windows).**

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
| I/O scheduling | `boost::asio` io_context (~500KB) | `io_thread_pool.hpp` — Standard C++ `ThreadPool` with `run_blocking()` coroutine bridging |
| CPU scheduling | `boost::asio::thread_pool` (no work-stealing) | `cpu_thread_pool.hpp` — Chase-Lev work-stealing deque |
| Process mgmt | `boost::process` (heavy dep) | `process.hpp` — Cross-platform RAII (POSIX `fork` / Win `CreateProcessA`), no zombies |
| Structured concurrency | None (C++26 proposal) | `TaskGroup` — spawn + wait + exception propagation |
| Cancellation | None (ad-hoc `atomic<bool>`) | `CancellationToken` — thread-safe, `throw_if_cancelled()` |

**No `boost`. No `asio`. No `libuv`. No `libcurl`. No exceptions for control flow.**

---

## Which Thread Pool Should I Use?

| Scenario | Use | Why |
|----------|-----|-----|
| **Network server / client** | `io_bound::ThreadPool` | Parks coroutines safely via `run_blocking()` while native blocking sockets send/recv data; leaves main thread responsive. |
| **HTTP API calls** | `io_bound::ThreadPool` | Bridge native HTTP requests using `co_await pool.run_blocking(...)` without stalling the event loop. |
| **IPC / stdio pipes** | `io_bound::ThreadPool` | `run_blocking()` safely wraps blocking pipe reads from subprocesses. |
| **Number crunching** | `cpu_bound::ThreadPool` | Chase-Lev work-stealing deque keeps CPU cache hot; no I/O overhead; `submit()` returns `std::future` |
| **Data processing pipelines** | `cpu_bound::ThreadPool` | `TaskGroup` for structured concurrency — spawn parallel stages, `co_await group.wait()` |
| **Mixed I/O + CPU** | **Both** | I/O pool handles network/subprocesses; CPU pool handles computation; communicate via `std::future` or shared state |
| **Spawning child processes** | `process::Process` | RAII wrapper — no zombies, noexcept `terminate()`, idempotent `wait()` |
| **Fire-and-forget background tasks** | Either pool's `spawn()` | `DetachedTask` auto-destroys on completion; no leaks, no manual cleanup |

### Decision Flowchart

```
Is the work I/O-bound (network, pipes, files)?
├── Yes → io_bound::ThreadPool
│         (Standard threads, run_blocking() coroutine bridge)
│
└── No → Is it CPU-bound (computation, algorithms)?
    ├── Yes → cpu_bound::ThreadPool
    │         (Chase-Lev work-stealing, TaskGroup)
    │
    └── No → Do you need to spawn processes?
        └── Yes → process::Process
                  (fork/exec, CreateProcessA, RAII)
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
│  ThreadPool          │   │  ChaseLevDeque           │
│   (Standard C++)     │   │   (lock-free work-steal) │
│  run_blocking()      │   │  ThreadPool              │
│   (Park coroutines)  │   │   (run / spawn / submit) │
│  submit / run        │   │  TaskGroup               │
│                      │   │   (structured conc.)     │
│  Cross-platform      │   │                          │
│  Pragmatic simplicity│   │  No reactor — pure CPU   │
└──────────────────────┘   └──────────────────────────┘

┌──────────────────────┐
│     process.hpp      │
│                      │
│  Process (RAII)      │
│   fork + execvp      │
│   CreateProcessA     │
│   waitpid / WaitFor  │
│   kill(SIGTERM)      │
│   close_stdin()      │
│   read_stdout()      │
│   write_stdin()      │
│                      │
│  FileDescriptor      │
│   (RAII fd wrapper)  │
│                      │
│  No zombies          │
│  Noexcept terminate  │
│  Cross-platform      │
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

- **`ThreadPool`** — standard C++ thread pool using `std::thread` and `std::mutex`. Highly portable across Mac, Linux, and Windows.
- **`run_blocking()`** — coroutine awaitable that safely parks the coroutine, executes a blocking OS call (like `::recv()` or `::read()`) on a worker thread, and resumes the coroutine when data is ready.
- **`submit()` / `run()`** — execute plain callables or `AsyncTask<T>` coroutines on the pool, returning `std::future`.
- **`std::expected<T, std::error_code>`** — all I/O wrappers return expected, never throw.

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
  - `start(executable, args)` — `fork()` + `execvp()` (POSIX) or `CreateProcessA` (Windows)
  - `wait()` — `waitpid()` / `WaitForSingleObject()`, returns exit code; idempotent
  - `terminate()` — `SIGTERM` / `TerminateProcess`; `noexcept` — safe in destructors
  - `running()` — checks status and reaps zombies (no `ECHILD` on later `wait()`)
  - `close_stdin()` — signals EOF to child
  - `read_stdout()` — blocking read returning `std::expected<std::size_t, std::error_code>` (perfect for `io_bound::run_blocking()`)
  - `write_stdin()` — blocking write returning `std::expected<std::size_t, std::error_code>`
- **`FileDescriptor`** — RAII wrapper for native OS handles; move-only
- **No zombies, no orphans** — destructor calls `terminate()` + `wait()` if child still running

---

## Requirements

- **C++23** (`std::expected`, `std::coroutine`, `std::span`, `std::binary_semaphore`, `std::format`)
- Clang 16+ (macOS/Linux), GCC 13+ (Linux), MSVC 19.34+ (Windows)
- **Cross-Platform**: Mac, Linux, and Windows Native
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

**Windows (MSVC):**
```powershell
cl /std:c++latest /EHsc /I include src\io_thread_pool_test.cpp /out:bin\io_test.exe
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
| `ThreadPool` (I/O) | `io_thread_pool.hpp` | `io_bound` | Standard thread pool with coroutine bridging |
| `run_blocking()` | `io_thread_pool.hpp` | `io_bound` | Parks coroutine during blocking I/O |
| `ChaseLevDeque` | `cpu_thread_pool.hpp` | `cpu_bound` | Lock-free work-stealing deque |
| `ThreadPool` (CPU) | `cpu_thread_pool.hpp` | `cpu_bound` | Work-stealing scheduler |
| `TaskGroup` | `cpu_thread_pool.hpp` | `cpu_bound` | Structured concurrency |
| `Process` | `process.hpp` | `process` | Cross-platform RAII process lifecycle |
| `FileDescriptor` | `process.hpp` | `process` | RAII native handle wrapper |

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

// sync_wait — block until done (use only on non-worker threads)
int result = sync_wait(compute());
```

### I/O Thread Pool (io_thread_pool.hpp)

```cpp
#include "io_thread_pool.hpp"

using namespace pooriayousefi::io_bound;
using namespace pooriayousefi::core;

// Cross-platform thread pool for I/O
ThreadPool pool{4};

auto network_task = [&]() -> AsyncTask<void> {
    // Create a native socket (not shown)
    int sock = ...; 

    // Park the coroutine while a blocking OS call runs on a worker thread
    auto bytes = co_await pool.run_blocking([sock]() -> std::expected<std::size_t, std::error_code> {
        char buf[4096];
        int n = ::recv(sock, buf, sizeof(buf), 0);
        if (n < 0) return std::unexpected(std::make_error_code(std::errc::io_error));
        return static_cast<std::size_t>(n);
    });

    if (bytes && *bytes > 0) {
        // Handle data
    }
};

auto fut = pool.run(network_task());
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

// Spawn a child process with piped stdin/stdout (Cross-platform)
Process p;
p.start("cat", {});

// Write to child's stdin (blocking)
std::string msg = "hello\n";
p.write_stdin(std::as_bytes(std::span{msg}));
p.close_stdin();  // signal EOF so cat exits

// Read from child's stdout (blocking - ideal for io_bound::run_blocking)
std::byte buf[256];
auto res = p.read_stdout(buf);

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

// I/O pool for network/pipes, CPU pool for computation
io_bound::ThreadPool io_pool{2};  // fewer threads — I/O is mostly waiting
cpu_bound::ThreadPool cpu_pool{4}; // more threads — CPU-bound work

// Spawn an MCP server as a subprocess
process::Process mcp_server;
mcp_server.start("npx", {"-y", "@modelcontextprotocol/server-filesystem", "/tmp"});

// Use io_pool.run_blocking() to read from mcp_server.read_stdout()
// Use cpu_pool to run heavy computation on the data received
```

---

## Comparison

| Feature | PooriAsync | boost::asio | libuv | tokio (Rust) |
|---------|-----------|-------------|-------|-------------|
| **Dependencies** | Zero | Boost (~20MB) | libuv (~1MB) | Rust stdlib + tokio |
| **Coroutines** | C++23 native | `co_await` (asio-specific) | Callbacks | `async/await` |
| **I/O model** | Standard Threads + `run_blocking()` | epoll/kqueue/io_uring | epoll/kqueue | epoll/kqueue/io_uring |
| **CPU work-stealing** | Chase-Lev deque | No (thread_pool only) | No | Yes (tokio + rayon) |
| **Structured concurrency** | `TaskGroup` | No | No | `JoinSet` |
| **Process mgmt** | Cross-platform RAII | `boost::process` (heavy) | `uv_spawn` | `std::process::Command` |
| **Cancellation** | `CancellationToken` | No | `uv_cancel_t` | `CancellationToken` |
| **Error handling** | `std::expected` | Exceptions / `error_code` | int return codes | `Result<T, E>` |
| **Header-only** | Yes | No (compiled lib) | No | No |
| **Binary size** | Minimal | ~500KB+ | ~100KB+ | Large |
| **Compile time** | Fast | Slow (template explosion) | Fast | Fast |
| **Platform** | Mac / Linux / Windows | Cross-platform | Cross-platform | Cross-platform |

---

## Limitations and Gotchas

1. **No TLS/SSL.** Native sockets are plaintext. For production over public networks, add TLS (OpenSSL or platform APIs).
2. **`sync_wait` deadlocks.** Calling `sync_wait` inside a `ThreadPool` worker thread blocks that worker, preventing the coroutines scheduled on that pool from ever resuming. Use `ThreadPool::run()` from the main thread instead.
3. **Coroutine frame heap allocation.** Each `co_await` creates a frame on the heap. For ultra-low-latency, add a pooled allocator.
4. **`wait()` can deadlock.** Calling `cpu_bound::ThreadPool::wait()` from a worker thread blocks that worker. Call only from non-worker threads.
5. **`TaskGroup` stores only first exception.** Subsequent exceptions are swallowed.
6. **`ChaseLevDeque` has fixed capacity (1024).** Overflow falls back to the global queue — no data loss, but reduced locality.
7. **`Process` destructor terminates running children.** If the `Process` object is destroyed while the child is still running, it sends `SIGTERM` / `TerminateProcess` and reaps. Call `wait()` first for graceful shutdown.
8. **No `stderr` pipe.** `Process` pipes stdin and stdout only. stderr is inherited from the parent.

---

## License

Apache License 2.0

---

**Author:** Pooria Yousefi

---