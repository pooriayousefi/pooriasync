#include "io_thread_pool.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <print>
#include <string>
#include <thread>
#include <vector>

using namespace pooriayousefi::io_bound;
using namespace pooriayousefi::core;

void print_test_header(const std::string& test_name)
{
    std::println("\n--- Running Test: {} ---", test_name);
}

bool test_thread_pool_basic()
{
    bool success = true;
    print_test_header("ThreadPool Basic (submit + future)");

    ThreadPool pool{4};

    auto f1 = pool.submit(
        [](int a, int b) -> int
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            return a + b;
        },
        10,
        32
    );

    auto f2 = pool.submit(
        []() -> std::string
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return std::string{"hello from pool"};
        }
    );

    int sum = f1.get();
    std::string msg = f2.get();

    std::println("Sum: {} (expected 42)", sum);
    std::println("Message: {}", msg);

    if (sum != 42 || msg != "hello from pool")
    {
        success = false;
    }

    return success;
}

bool test_thread_pool_exception()
{
    bool success = true;
    print_test_header("ThreadPool Exception Propagation");

    ThreadPool pool{2};

    auto f = pool.submit(
        []() -> int
        {
            throw std::runtime_error{"intentional failure"};
        }
    );

    bool caught = false;
    try
    {
        static_cast<void>(f.get());
    }
    catch (const std::exception& e)
    {
        std::println("Caught exception: {}", e.what());
        caught = true;
    }

    if (!caught)
    {
        std::println("Error: expected exception was not propagated.");
        success = false;
    }

    return success;
}

bool test_coroutine_run()
{
    bool success = true;
    print_test_header("Coroutine run (AsyncTask on ThreadPool)");

    ThreadPool pool{4};

    auto compute = [&pool]() -> AsyncTask<int>
    {
        co_await pool.schedule();
        co_return 123;
    };

    std::future<int> fut = pool.run(compute());

    int result = fut.get();
    std::println("Coroutine result: {} (expected 123)", result);

    if (result != 123)
    {
        success = false;
    }

    return success;
}

bool test_coroutine_schedule()
{
    bool success = true;
    print_test_header("Coroutine schedule (co_await pool.schedule())");

    ThreadPool pool{4};

    auto task = [&pool]() -> AsyncTask<int>
    {
        std::thread::id id1 = std::this_thread::get_id();
        co_await pool.schedule();
        std::thread::id id2 = std::this_thread::get_id();

        if (id1 == id2)
        {
            co_return -1; // Should have switched threads.
        }
        co_return 42;
    };

    std::future<int> fut = pool.run(task());
    int result = fut.get();

    std::println("Schedule result: {} (expected 42)", result);

    if (result != 42)
    {
        success = false;
    }

    return success;
}

bool test_coroutine_exception()
{
    bool success = true;
    print_test_header("Coroutine Exception Propagation");

    ThreadPool pool{2};

    auto throwing = [&pool]() -> AsyncTask<int>
    {
        co_await pool.schedule();
        throw std::runtime_error{"coroutine failure"};
        co_return 0;
    };

    std::future<int> fut = pool.run(throwing());

    bool caught = false;
    try
    {
        static_cast<void>(fut.get());
    }
    catch (const std::exception& e)
    {
        std::println("Caught: {}", e.what());
        caught = true;
    }

    if (!caught)
    {
        success = false;
    }

    return success;
}

bool test_sync_wait()
{
    bool success = true;
    print_test_header("sync_wait (blocking on a coroutine)");

    ThreadPool pool{4};

    auto slow_task = [&pool]() -> AsyncTask<int>
    {
        co_await pool.schedule();
        co_return 99;
    };

    auto fut = pool.run(slow_task());
    int val = fut.get();

    std::println("sync_wait result: {} (expected 99)", val);

    if (val != 99)
    {
        success = false;
    }

    return success;
}

bool test_fire_and_forget()
{
    bool success = true;
    print_test_header("FireAndForget (eager start, self-managed)");

    ThreadPool pool{2};

    std::atomic<int> counter{0};

    auto ff = [&counter, &pool]() -> FireAndForget
    {
        // schedule to the pool to do the work
        co_await pool.schedule();
        counter.store(42, std::memory_order_release);
    };

    ff().detach();

    // Wait for the counter to be set.
    for (int i = 0; i < 100 && counter.load(std::memory_order_acquire) != 42; ++i)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    int val = counter.load(std::memory_order_acquire);
    std::println("Counter: {} (expected 42)", val);

    if (val != 42)
    {
        success = false;
    }

    return success;
}

bool test_async_generator()
{
    bool success = true;
    print_test_header("AsyncGenerator (synchronous iteration)");

    auto gen = []() -> AsyncGenerator<int>
    {
        for (int i = 1; i <= 5; ++i)
        {
            co_yield i * 10;
        }
    };

    int sum = 0;
    for (int val : gen())
    {
        std::println("  yielded: {}", val);
        sum += val;
    }

    std::println("Sum of yielded values: {} (expected 150)", sum);

    if (sum != 150)
    {
        success = false;
    }

    return success;
}

bool test_cancellation_token()
{
    bool success = true;
    print_test_header("CancellationToken");

    CancellationToken token;

    if (token.is_cancelled())
    {
        std::println("Error: token should not be cancelled initially.");
        success = false;
    }

    token.cancel();

    if (!token.is_cancelled())
    {
        std::println("Error: token should be cancelled after cancel().");
        success = false;
    }

    bool caught = false;
    try
    {
        token.throw_if_cancelled();
    }
    catch (const CancelledException& e)
    {
        std::println("Caught: {}", e.what());
        caught = true;
    }

    if (!caught)
    {
        std::println("Error: throw_if_cancelled() did not throw.");
        success = false;
    }

    return success;
}

bool test_async_pipe_ipc()
{
    bool success = true;
    print_test_header("AsyncPipe IPC (write → read)");

    ThreadPool pool{2};

    auto pipe_task = [&pool]() -> AsyncTask<std::expected<std::string, std::error_code>>
    {
        AsyncPipe pipe{NetworkReactor::current};

        // On POSIX, create a real pipe for testing.
        int fds[2];
        if (::pipe(fds) != 0)
        {
            co_return std::unexpected(std::make_error_code(static_cast<std::errc>(errno)));
        }
        pipe.assign_read(fds[0]);
        pipe.assign_write(fds[1]);

        std::string_view message = "hello pipe!";
        auto send_result = co_await pipe.send(std::as_bytes(std::span{message}));
        if (!send_result)
        {
            co_return std::unexpected(send_result.error());
        }

        std::byte buf[64] = {};
        auto recv_result = co_await pipe.recv(std::span<std::byte>{buf, sizeof(buf)});
        if (!recv_result)
        {
            co_return std::unexpected(recv_result.error());
        }

        std::string received(
            reinterpret_cast<const char*>(buf),
            *recv_result
        );

        co_return received;
    };

    auto fut = pool.run(pipe_task());
    auto result = fut.get();

    if (!result)
    {
        std::println("Pipe error: {}", result.error().message());
        success = false;
    }
    else
    {
        std::println("Received: {}", *result);
        if (*result != "hello pipe!")
        {
            std::println("Content mismatch!");
            success = false;
        }
    }

    return success;
}

bool test_move_only_function()
{
    bool success = true;
    print_test_header("MoveOnlyFunction");

    std::atomic<int> counter{0};
    auto ptr = std::make_shared<int>(42);

    MoveOnlyFunction func = [&counter, ptr]() mutable
    {
        counter.store(*ptr, std::memory_order_release);
    };

    if (!func)
    {
        std::println("Error: MoveOnlyFunction should be valid.");
        success = false;
    }

    func();

    int val = counter.load(std::memory_order_acquire);
    std::println("Counter: {} (expected 42)", val);

    if (val != 42)
    {
        success = false;
    }

    MoveOnlyFunction moved = std::move(func);
    if (func)
    {
        std::println("Error: moved-from function should be empty.");
        success = false;
    }

    moved();

    return success;
}

int main()
{
    int exit_code = EXIT_SUCCESS;

    try
    {
        bool all_passed = true;

        if (!test_thread_pool_basic())
        {
            all_passed = false;
        }
        if (!test_thread_pool_exception())
        {
            all_passed = false;
        }
        if (!test_coroutine_run())
        {
            all_passed = false;
        }
        if (!test_coroutine_schedule())
        {
            all_passed = false;
        }
        if (!test_coroutine_exception())
        {
            all_passed = false;
        }
        if (!test_sync_wait())
        {
            all_passed = false;
        }
        if (!test_fire_and_forget())
        {
            all_passed = false;
        }
        if (!test_async_generator())
        {
            all_passed = false;
        }
        if (!test_cancellation_token())
        {
            all_passed = false;
        }
        if (!test_async_pipe_ipc())
        {
            all_passed = false;
        }
        if (!test_move_only_function())
        {
            all_passed = false;
        }

        std::println("\n==============================");
        if (all_passed)
        {
            std::println("ALL POORIASYNC TESTS PASSED!");
        }
        else
        {
            std::println("SOME POORIASYNC TESTS FAILED!");
            exit_code = EXIT_FAILURE;
        }
        std::println("==============================");
    }
    catch (const std::exception& e)
    {
        std::println("Exception occurred: {}", e.what());
        exit_code = EXIT_FAILURE;
    }

    return exit_code;
}