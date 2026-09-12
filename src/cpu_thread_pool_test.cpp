#include "cpu_thread_pool.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <print>
#include <string>
#include <thread>
#include <vector>

using namespace pooriayousefi::cpu_bound;
using namespace pooriayousefi::core;

void print_test_header(const std::string& test_name)
{
    std::println("\n--- Running Test: {} ---", test_name);
}

bool test_submit_basic()
{
    bool success = true;
    print_test_header("submit() Basic (plain callable + future)");

    ThreadPool pool{4};

    auto f1 = pool.submit(
        [](int a, int b) -> int
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            return a + b;
        },
        10,
        32
    );

    auto f2 = pool.submit(
        []() -> std::string
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return std::string{"hello from cpu pool"};
        }
    );

    int sum = f1.get();
    std::string msg = f2.get();

    std::println("Sum: {} (expected 42)", sum);
    std::println("Message: {}", msg);

    if (sum != 42 || msg != "hello from cpu pool")
    {
        success = false;
    }

    return success;
}

bool test_submit_exception()
{
    bool success = true;
    print_test_header("submit() Exception Propagation");

    ThreadPool pool{2};

    auto f = pool.submit(
        []() -> int
        {
            throw std::runtime_error{"intentional cpu pool failure"};
        }
    );

    bool caught = false;
    try
    {
        static_cast<void>(f.get());
    }
    catch (const std::exception& e)
    {
        std::println("Caught: {}", e.what());
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
    print_test_header("Coroutine run() (AsyncTask on CPU pool)");

    ThreadPool pool{4};

    auto compute = [&pool]() -> AsyncTask<int>
    {
        co_await pool.schedule();
        int result = 0;
        for (int i = 1; i <= 100; ++i)
        {
            result += i;
        }
        co_return result;
    };

    std::future<int> fut = pool.run(compute());
    int result = fut.get();

    std::println("Coroutine result: {} (expected 5050)", result);

    if (result != 5050)
    {
        success = false;
    }

    return success;
}

bool test_coroutine_schedule()
{
    bool success = true;
    print_test_header("Coroutine schedule() (thread switch)");

    ThreadPool pool{4};

    auto task = [&pool]() -> AsyncTask<bool>
    {
        std::thread::id id1 = std::this_thread::get_id();
        co_await pool.schedule();
        std::thread::id id2 = std::this_thread::get_id();

        co_return (id1 != id2);
    };

    std::future<bool> fut = pool.run(task());
    bool result = fut.get();

    std::println("Thread switched: {} (expected true)", result);

    if (!result)
    {
        success = false;
    }

    return success;
}

bool test_coroutine_exception()
{
    bool success = true;
    print_test_header("Coroutine run() Exception Propagation");

    ThreadPool pool{2};

    auto throwing = [&pool]() -> AsyncTask<int>
    {
        co_await pool.schedule();
        throw std::runtime_error{"coroutine failure in cpu pool"};
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

bool test_spawn_fire_and_forget()
{
    bool success = true;
    print_test_header("spawn() (fire-and-forget AsyncTask)");

    ThreadPool pool{4};

    std::atomic<int> counter{0};

    auto task = [&pool, &counter]() -> AsyncTask<void>
    {
        co_await pool.schedule();
        counter.fetch_add(1, std::memory_order_release);
    };

    pool.spawn(task());
    pool.spawn(task());
    pool.spawn(task());

    pool.wait();

    int val = counter.load(std::memory_order_acquire);
    std::println("Counter: {} (expected 3)", val);

    if (val != 3)
    {
        success = false;
    }

    return success;
}

bool test_wait_blocks()
{
    bool success = true;
    print_test_header("wait() (blocking until all complete)");

    ThreadPool pool{4};

    std::atomic<int> counter{0};

    auto task = [&pool, &counter]() -> AsyncTask<void>
    {
        co_await pool.schedule();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        counter.fetch_add(1, std::memory_order_release);
    };

    for (int i = 0; i < 10; ++i)
    {
        pool.spawn(task());
    }

    pool.wait();

    int val = counter.load(std::memory_order_acquire);
    std::println("Completed {} tasks (expected 10)", val);

    if (val != 10)
    {
        success = false;
    }

    return success;
}

bool test_task_group()
{
    bool success = true;
    print_test_header("TaskGroup (structured concurrency)");

    ThreadPool pool{4};
    TaskGroup group{pool};

    std::atomic<int> sum{0};

    auto task = [&pool, &sum](int n) -> AsyncTask<void>
    {
        co_await pool.schedule();
        sum.fetch_add(n, std::memory_order_release);
    };

    for (int i = 1; i <= 10; ++i)
    {
        group.spawn(task(i));
    }

    auto waiter = [&group]() -> AsyncTask<void>
    {
        co_await group.wait();
    };

    auto fut = pool.run(waiter());
    fut.get();

    int total = sum.load(std::memory_order_acquire);
    std::println("Sum of 1..10 = {} (expected 55)", total);

    if (total != 55)
    {
        success = false;
    }

    return success;
}

bool test_task_group_exception()
{
    bool success = true;
    print_test_header("TaskGroup Exception Propagation");

    ThreadPool pool{4};
    TaskGroup group{pool};

    auto good_task = [&pool]() -> AsyncTask<void>
    {
        co_await pool.schedule();
    };

    auto bad_task = [&pool]() -> AsyncTask<void>
    {
        co_await pool.schedule();
        throw std::runtime_error{"task group child failure"};
    };

    group.spawn(good_task());
    group.spawn(bad_task());
    group.spawn(good_task());

    auto waiter = [&group]() -> AsyncTask<void>
    {
        co_await group.wait();
    };

    auto fut = pool.run(waiter());

    bool caught = false;
    try
    {
        fut.get();
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

bool test_work_stealing()
{
    bool success = true;
    print_test_header("Work Stealing (uneven load distribution)");

    ThreadPool pool{4};

    std::atomic<int> completed{0};

    auto heavy_task = [&pool, &completed]() -> AsyncTask<void>
    {
        co_await pool.schedule();
        // Simulate CPU work.
        volatile int dummy = 0;
        for (int i = 0; i < 100000; ++i)
        {
            dummy += i;
        }
        (void)dummy;
        completed.fetch_add(1, std::memory_order_release);
    };

    // Spawn many tasks — workers should steal from each other.
    for (int i = 0; i < 50; ++i)
    {
        pool.spawn(heavy_task());
    }

    pool.wait();

    int count = completed.load(std::memory_order_acquire);
    std::println("Completed {} / 50 tasks", count);

    if (count != 50)
    {
        success = false;
    }

    return success;
}

bool test_mixed_submit_and_coroutine()
{
    bool success = true;
    print_test_header("Mixed submit() + run() (plain + coroutine)");

    ThreadPool pool{4};

    // Submit a plain callable.
    auto plain_fut = pool.submit(
        [](int n) -> int
        {
            int result = 1;
            for (int i = 2; i <= n; ++i)
            {
                result *= i;
            }
            return result;
        },
        5
    );

    // Run a coroutine that depends on the plain result.
    auto coro = [&pool, &plain_fut]() -> AsyncTask<std::string>
    {
        co_await pool.schedule();
        int fact = plain_fut.get();
        co_return std::format("5! = {}", fact);
    };

    auto coro_fut = pool.run(coro());
    std::string result = coro_fut.get();

    std::println("Result: {} (expected '5! = 120')", result);

    if (result != "5! = 120")
    {
        success = false;
    }

    return success;
}

int main()
{
    int exit_code = EXIT_SUCCESS;

    try
    {
        bool all_passed = true;

        if (!test_submit_basic())
        {
            all_passed = false;
        }
        if (!test_submit_exception())
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
        if (!test_spawn_fire_and_forget())
        {
            all_passed = false;
        }
        if (!test_wait_blocks())
        {
            all_passed = false;
        }
        if (!test_task_group())
        {
            all_passed = false;
        }
        if (!test_task_group_exception())
        {
            all_passed = false;
        }
        if (!test_work_stealing())
        {
            all_passed = false;
        }
        if (!test_mixed_submit_and_coroutine())
        {
            all_passed = false;
        }

        std::println("\n==============================");
        if (all_passed)
        {
            std::println("ALL CPU THREAD POOL TESTS PASSED!");
        }
        else
        {
            std::println("SOME CPU THREAD POOL TESTS FAILED!");
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