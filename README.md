
# PooriAsync

A single-header, dependency-free C++23 coroutine and async IO thread pool library. 
It provides a foundational `AsyncTask`, `AsyncGenerator`, a cross-platform `NetworkReactor` (using `epoll`/`kqueue`/`select`),
and an `AsyncPipe` class for non-blocking IPC and `stdio` communication — all with strict memory safety (`std::span`)
and error handling (`std::expected`).

```cpp
#include "io_thread_pool.hpp"

using namespace pooriayousefi::core;
using namespace pooriayousefi::io_bound;

ThreadPool pool{4};

AsyncTask<void> fetch_data() {
    co_await pool.schedule(); // Hop to the thread pool
    // ... do non-blocking work ...
    co_return;
}

sync_wait(fetch_data()); // Block until complete
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
  - [Usage](#usage)
    - [AsyncTasks and sync\_wait](#asynctasks-and-sync_wait)
    - [Thread Pool Scheduling](#thread-pool-scheduling)
    - [Network Reactor and Sockets](#network-reactor-and-sockets)
    - [Async IPC Pipes](#async-ipc-pipes)
  - [Limitations and Gotchas](#limitations-and-gotchas)
  - [License](#license)

---

## Features

- **C++23 Coroutines:** `AsyncTask<T>`, `AsyncGenerator<T>`, and `FireAndForget` primitives with symmetric transfer for continuation.
- **Cross-Platform Reactor:** `NetworkReactor` wraps `epoll` (Linux), `kqueue` (Mac/BSD), and `select` (Windows) behind a unified, non-blocking API.
- **Integrated Thread Pool:** A multi-worker thread pool that drives the reactor and executes coroutines asynchronously.
- **Async IPC (`AsyncPipe`):** Non-blocking inter-process communication for `stdio` transport, fully integrated with the reactor event loop.
- **Enterprise-Grade Safety:** Uses `std::span` for buffer memory safety and `std::expected` for exception-free, explicit error propagation in network and pipe I/O.
- **Cancellation Support:** `CancellationToken` integration to gracefully cancel in-flight coroutines.
- **Zero Dependencies:** Relies only on the C++23 standard library and native OS networking APIs.

## Requirements

Requires **C++23** (`std::expected`, `std::coroutine`, `std::format`, `<ranges>`, `std::span`). 
Tested with GCC 13+, Clang 16+, and MSVC 19.34+.

## Project Structure

```text
pooriasync/
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
clang++ -std=c++23 -O3 -I include src/main.cpp -o bin/pooriasync_test
./bin/pooriasync_test
```

**Windows (PowerShell):**
```powershell
cl /std:c++23 /EHsc /I include src/main.cpp /out:bin\pooriasync_test.exe
.\bin\pooriasync_test.exe
```

---

## Architecture

`asyncore.hpp` provides the primitive types that manage coroutine frames, promise objects, and continuations. `io_thread_pool.hpp` builds on this by providing a `NetworkReactor` that monitors OS-level file descriptors/sockets and resumes coroutines when data is ready to read/write.

The `ThreadPool` distributes coroutines across worker threads. When an `AsyncSocket` or `AsyncPipe` awaits a read/write event, it registers its coroutine handle with the thread-local `NetworkReactor`, yielding execution back to the reactor's event loop so it can process other connections.

## Usage

### AsyncTasks and sync_wait

`AsyncTask<T>` allows you to write asynchronous code that can be awaited. `sync_wait` bridges the async world back to synchronous code by blocking the calling thread.

```cpp
AsyncTask<int> compute_value(ThreadPool& pool) {
    co_await pool.schedule();
    co_return 42;
}

int result = sync_wait(compute_value(pool));
```

### Thread Pool Scheduling

Coroutines can be explicitly scheduled onto the thread pool using `co_await pool.schedule()`. `FireAndForget` coroutines can be detached to run independently.

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

---

## Limitations and Gotchas

1. **Windows `select` limit:** On Windows, the reactor uses `select()` for sockets, which is limited to `FD_SETSIZE` (usually 1024) concurrent sockets. 
2. **Windows IPC Pipes:** Because `select()` cannot monitor anonymous pipes, `AsyncPipe` on Windows utilizes a dedicated background thread per read operation to maintain async semantics without requiring IOCP. 
3. **`sync_wait` Deadlocks:** Calling `sync_wait` on an `AsyncTask` *inside* a `NetworkReactor::run()` loop will deadlock the reactor thread. `sync_wait` should only be used on main/CLI threads or outside the reactor's execution context.
4. **Thread-Local Reactor:** `AsyncSocket` and `AsyncPipe` rely on `current_reactor`, which is set `thread_local` inside `NetworkReactor::run()`. You cannot use them on a thread that is not part of the `ThreadPool`.
5. **Coroutine Lifetime:** `FireAndForget` coroutines destroy themselves upon completion. Ensure they do not capture references to local variables that might go out of scope. When manually starting a `FireAndForget`, save the handle, call `detach()`, and then `resume()`.

## License

Apache License 2.0 — see the headers of the `.hpp` files.
```