#include "io_thread_pool.hpp"
#include <print>
#include <string>
#include <array>

using namespace pooriayousefi::core;
using namespace pooriayousefi::io_bound;

// Helper function to print test headers
void print_test_header(const std::string& test_name)
{
    std::println("\n--- Running Test: {} ---", test_name);
}

// Test 1: ThreadPool::submit (Standard Synchronous Task)
bool test_thread_pool_submit()
{
    bool success = true;
    print_test_header("ThreadPool::submit");

    ThreadPool pool{4};
    
    auto fut = pool.submit([]() -> int
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return 42;
    });

    int result = fut.get();
    std::println("Result from submitted task: {}", result);

    if (result != 42)
    {
        success = false;
    }
    
    return success;
}

// Test 2: AsyncTask and sync_wait
AsyncTask<int> async_computation(ThreadPool& pool, int value)
{
    co_await pool.schedule();
    int result = value * 2;
    co_return result;
}

bool test_async_task_and_sync_wait()
{
    bool success = true;
    print_test_header("AsyncTask and sync_wait");

    ThreadPool pool{4};
    
    int result = sync_wait(async_computation(pool, 21));
    
    std::println("Result from async task: {}", result);

    if (result != 42)
    {
        success = false;
    }
    
    return success;
}

// Test 3: FireAndForget coroutine execution
FireAndForget fire_and_forget_task(ThreadPool& pool, std::atomic<int>& counter)
{
    co_await pool.schedule();
    counter.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

bool test_fire_and_forget()
{
    bool success = true;
    print_test_header("FireAndForget Task");

    ThreadPool pool{4};
    std::atomic<int> counter{0};

    // To properly use FireAndForget with std::suspend_always:
    // 1. Create the coroutine (starts suspended).
    // 2. Save the handle to a local variable.
    // 3. Detach it so the FireAndForget destructor doesn't destroy the frame.
    // 4. Resume the saved handle to start execution.
    // 5. The FinalAwaitable will safely destroy the frame when it finishes.
    for (int i = 0; i < 5; ++i)
    {
        auto ff = fire_and_forget_task(pool, counter);
        auto h = ff.handle; // Save handle before detaching!
        ff.detach();
        h.resume();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    std::println("Counter value: {}", counter.load());

    if (counter.load() != 5)
    {
        success = false;
    }
    
    return success;
}

// Test 4: AsyncGenerator
AsyncGenerator<int> generate_numbers(int count)
{
    for (int i = 1; i <= count; ++i)
    {
        co_yield i;
    }
    co_return;
}

bool test_async_generator()
{
    bool success = true;
    print_test_header("AsyncGenerator");

    int sum = 0;
    for (int val : generate_numbers(5))
    {
        sum += val;
    }
    
    std::println("Sum of generated numbers: {}", sum);

    if (sum != 15)
    {
        success = false;
    }
    
    return success;
}

// Main entry point
int main()
{
    int exit_code = EXIT_SUCCESS;

    try
    {
        bool all_passed = true;

        if (!test_thread_pool_submit()) { all_passed = false; }
        if (!test_async_task_and_sync_wait()) { all_passed = false; }
        if (!test_fire_and_forget()) { all_passed = false; }
        if (!test_async_generator()) { all_passed = false; }

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