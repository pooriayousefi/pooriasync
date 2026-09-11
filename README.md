
# PooriAsync

A two-header, dependency-free C++23 coroutine and async I/O library. `asyncore.hpp` provides foundational coroutine primitives (`AsyncTask`, `AsyncGenerator`, `FireAndForget`, `sync_wait`, `CancellationToken`, `MoveOnlyFunction`). `io_thread_pool.hpp` builds on this with a cross-platform `NetworkReactor` (epoll/kqueue/select), `AsyncSocket`, `AsyncPipe`, and a `ThreadPool` — all with strict memory safety (`std::span`) and explicit error handling (`std::expected`).

```cpp
#include "io_thread_pool.hpp"

using namespace pooriayousefi::core;
using namespace pooriayousefi::io_bound;

ThreadPool pool{4};

AsyncTask<int> compute() {
    co_await pool.schedule(); // Hop to a worker thread
    co_return 42;
}

int main() {
    std::future<int> fut = pool.run(compute());
    std::println("Result: {}", fut.get());
}
```

---

## Table of contents

- [PooriAsync](#pooriasync)
  - [Table of contents](#table-of-contents)
  - [Features](#features)
  - [Requirements](#requirements)
  - [Project Structure](#project-structure)
  - [Building](#building)
  - [Architecture](#architecture)
  - [Core Types (asyncore.hpp)](#core-types-asyncorehpp)
  - [IO Types (io\_thread\_pool.hpp)](#io-types-io_thread_poolhpp)
  - [Usage](#usage)
    - [AsyncTask and DetachedTask](#asynctask-and-detachedtask)
    - [AsyncGenerator](#asyncgenerator)
    - [FireAndForget](#fireandforget)
    - [Thread Pool Scheduling](#thread-pool-scheduling)
    - [Network Reactor and Sockets](#network-reactor-and-sockets)
    - [Async IPC Pipes](#async-ipc-pipes)
    - [Cancellation](#cancellation)
  - [Limitations and Gotchas](#limitations-and-gotchas)
  - [License](#license)

---

## Features

- **C++23 Coroutines:** `AsyncTask<T>`, `AsyncGenerator<T>`, `FireAndForget`, and `DetachedTask` primitives with symmetric transfer for zero-overhead continuations.
- **Cross-Platform Reactor:** `NetworkReactor` wraps `epoll` (Linux), `kqueue` (Mac/BSD), and `select` (Windows) behind a unified, non-blocking API.
- **Integrated Thread Pool:** A multi-worker thread pool that drives the reactor and executes coroutines asynchronously. Round-robin scheduling with `DetachedTask` for safe, leak-free coroutine execution.
- **Async IPC (`AsyncPipe`):** Non-blocking inter-process communication for `stdio` transport, fully integrated with the reactor event loop.
- **Async TCP (`AsyncSocket`):** Non-blocking connect, send, and recv with `std::expected` error handling.
- **Enterprise-Grade Safety:** Uses `std::span` for buffer memory safety and `std::expected` for exception-free, explicit error propagation in network and pipe I/O.
- **Cancellation Support:** `CancellationToken` with `cancel()`, `is_cancelled()`, and `throw_if_cancelled()`.
- **MoveOnlyFunction:** Type-erased, move-only callable wrapper (replacement for `std::function` when capturing move-only types).
- **Zero Dependencies:** Relies only on the C++23 standard library, `asyncore.hpp`, and native OS networking APIs.

## Requirements

Requires **C++23** (`std::expected`, `std::coroutine`, `std::format`, `std::span`, `std::binary_semaphore`).
Tested with GCC 13+, Clang 16+, and MSVC 19.34+.

## Project Structure

```text
pooriasync/
├── bin/
├── include/
│   ├── asyncore.hpp
│   └── io_thread_pool.hpp
├── src/
│   └── main.cpp
└── README.md
```

## Building

**Mac/Linux:**
```bash
mkdir -p bin
clang++ -std=c++23 -O3 -I include src/main.cpp -o bin/pooriasync_test
./bin/pooriasync_test
```

**Windows (PowerShell, MSVC):**
```powershell
if (-not (Test-Path bin)) { New-Item -ItemType Directory bin }
cl /std:c++23 /EHsc /I include src/main.cpp ws2_32.lib /out:bin\pooriasync_test.exe
.\bin\pooriasync_test.exe
```

> **Note:** On Windows, you must link `ws2_32.lib` (Winsock).

---

## Architecture

`asyncore.hpp` provides the primitive types that manage coroutine frames, promise objects, and continuations. `io_thread_pool.hpp` builds on this by providing a `NetworkReactor` that monitors OS-level file descriptors/sockets and resumes coroutines when data is ready to read/write.

The `ThreadPool` distributes coroutines across worker threads. When an `AsyncSocket` or `AsyncPipe` awaits a read/write event, it registers its coroutine handle with the thread-local `NetworkReactor`, yielding execution back to the reactor's event loop so it can process other connections.

---

## Core Types (asyncore.hpp)

| Type | Description |
|------|-------------|
| `AsyncTask<T>` | Lazy coroutine that returns a `T`. Awaits with symmetric transfer. Move-only. |
| `AsyncTask<void>` | Void specialization. |
| `AsyncGenerator<T>` | Lazy coroutine that yields multiple `T` values via `co_yield`. Iterable. |
| `FireAndForget` | Eager coroutine (starts immediately). Self-managed lifetime — destroy in destructor or detach. |
| `DetachedTask` | Lazy coroutine (starts suspended). Auto-destroys frame on completion via `FinalAwaitable`. Used by `ThreadPool::run()`. |
| `SyncWaitTask<T>` | Internal helper for `sync_wait()`. Uses `std::binary_semaphore` to block the calling thread. |
| `sync_wait()` | Blocks the current thread until the awaited coroutine completes. Returns the result. |
| `CancellationToken` | Thread-safe cancel flag (`std::atomic<bool>`). Non-copyable, non-movable. |
| `CancelledException` | Thrown by `throw_if_cancelled()`. Inherits `std::runtime_error`. |
| `MoveOnlyFunction` | Type-erased callable. Move-only. Wraps any callable in a `std::unique_ptr` (heap-allocated). |

## IO Types (io_thread_pool.hpp)

| Type | Description |
|------|-------------|
| `NetworkReactor` | Per-thread event loop. Uses epoll (Linux), kqueue (Mac/BSD), or select (Windows). Has `schedule()`, `register_socket()`, `deregister_socket()`, `run()`, `stop()`, `yield()`. |
| `ThreadPool` | Round-robin pool of `NetworkReactor` workers. Has `submit()`, `run()`, `schedule()`, `enqueue_raw()`. |
| `AsyncSocket` | Coroutine-based TCP socket with `async_connect()`, `send()`, `recv()`. Uses `std::expected` for errors. |
| `AsyncPipe` | Coroutine-based anonymous pipe for IPC/stdio. Same API as `AsyncSocket` but for pipes. |

---

## Usage

### AsyncTask and DetachedTask

`AsyncTask<T>` is a lazy coroutine — it starts suspended and must be resumed by someone (e.g., `ThreadPool::run()` or `co_await`).

```cpp
AsyncTask<int> compute(ThreadPool& pool) {
    co_await pool.schedule();
    co_return 42;
}

// Run on the pool — returns a std::future
std::future<int> fut = pool.run(compute(pool));
int result = fut.get();
```

`DetachedTask` is used internally by `ThreadPool::run()`. It starts suspended, resumes on a worker thread, and auto-destroys its frame when complete — no leaks, no dangling handles.

### AsyncGenerator

`AsyncGenerator<T>` yields values lazily. Iteration is synchronous (the generator suspends/resumes on each `++`):

```cpp
AsyncGenerator<int> count(int n) {
    for (int i = 1; i <= n; ++i) {
        co_yield i * 10;
    }
}

for (int val : count(5)) {
    std::cout << val << '\n';  // 10, 20, 30, 40, 50
}
```

### FireAndForget

`FireAndForget` starts **immediately** (eager). Detach to let it run independently:

```cpp
FireAndForget background_task(ThreadPool& pool, std::atomic<int>& counter) {
    co_await pool.schedule();
    counter.store(42, std::memory_order_release);
}

// Launch and forget:
auto ff = background_task(pool, counter);
ff.detach();
```

### Thread Pool Scheduling

`co_await pool.schedule()` suspends the current coroutine and enqueues its resume on a worker thread:

```cpp
AsyncTask<void> worker(ThreadPool& pool) {
    std::thread::id caller = std::this_thread::get_id();
    co_await pool.schedule();  // Switch to a worker thread
    std::thread::id worker = std::this_thread::get_id();
    assert(caller != worker);
    co_return;
}
```

### Network Reactor and Sockets

The `AsyncSocket` class handles TCP connections asynchronously. It uses `std::expected` to report connection or I/O errors without throwing exceptions.

```cpp
AsyncTask<void> connect_to_server() {
    AsyncSocket sock;
    auto connect_result = co_await sock.async_connect("localhost", 8080);
    if (!connect_result) {
        std::cerr << "Failed to connect\n";
        co_return;
    }
    
    std::array<std::byte, 1024> buffer{};
    auto recv_result = co_await sock.recv(buffer);
    if (recv_result && *recv_result > 0) {
        // Process data
    }
}
```

### Async IPC Pipes

`AsyncPipe` allows for non-blocking communication with child processes via `stdio`. On POSIX systems, pipe file descriptors are registered directly with `epoll`/`kqueue`.

```cpp
AsyncTask<void> pipe_round_trip(AsyncPipe& pipe) {
    std::string_view msg = "hello pipe!";
    auto send_res = co_await pipe.send(std::as_bytes(std::span{msg}));
    if (!send_res) { co_return; }

    std::byte buf[64] = {};
    auto recv_res = co_await pipe.recv(buf);
    if (recv_res && *recv_res > 0) {
        std::string received(reinterpret_cast<const char*>(buf), *recv_res);
    }
}
```

### Cancellation

`CancellationToken` is a thread-safe, one-way cancel flag:

```cpp
CancellationToken token;

// Check:
if (token.is_cancelled()) { /* abort */ }

// Throw:
token.throw_if_cancelled();  // throws CancelledException

// Cancel from another thread:
token.cancel();
```

---

## Limitations and Gotchas

1. **Windows `select` limit:** On Windows, the reactor uses `select()` for sockets, which is limited to `FD_SETSIZE` (usually 1024) concurrent sockets.
2. **Windows IPC Pipes:** `AsyncPipe` on Windows uses `PeekNamedPipe` + `co_await reactor->yield()` polling, because anonymous pipes are not compatible with `select()`. For high-performance Windows IPC, IOCP is required.
3. **Windows `wake_up()`:** There is no wake mechanism on Windows. The reactor relies on a 100ms `select()` timeout to pick up newly scheduled tasks. This adds up to 100ms latency for task scheduling on Windows.
4. **`sync_wait` Deadlocks:** Calling `sync_wait` on an `AsyncTask` *inside* a `NetworkReactor::run()` loop will deadlock the reactor thread. `sync_wait` should only be used on main/CLI threads or outside the reactor's execution context.
5. **Thread-Local Reactor:** `AsyncSocket` and `AsyncPipe` rely on `NetworkReactor::current` (thread-local), which is set inside `NetworkReactor::run()`. You cannot use them on a thread that is not part of the `ThreadPool`.
6. **Coroutine Lifetime:** `FireAndForget` coroutines start eagerly and self-manage their frame. Ensure they do not capture references to local variables that might go out of scope. `DetachedTask` starts suspended and auto-destroys on completion — safe for cross-thread resume.
7. **`AsyncTask` move assignment deleted:** `AsyncTask` can be moved but not reassigned. This prevents reassignment of an active task that might be mid-await.
8. **`MoveOnlyFunction` always heap-allocates:** No small-buffer-optimization (SBO). Every callable is wrapped in a `std::unique_ptr`. For high-frequency task dispatch, consider a pooled allocator.
9. **`std::strerror` not thread-safe:** Error messages in `pooriprocess.hpp` use `strerror(errno)` which writes to a static buffer. The message is consumed immediately, so the race window is minimal.
10. **macOS `kevent` timeout:** The reactor uses a 100ms `timespec` timeout on macOS to prevent indefinite blocking when no events arrive. This ensures `stopped_` is checked regularly.

---

## License

Apache License 2.0

---

**Author:** Pooria Yousefi

---