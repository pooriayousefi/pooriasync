// ============================================================================
//  cpu_thread_pool.hpp — CPU-Bound Thread Pool with Work-Stealing
//  Developed by: Pooria Yousefi
//  License: Apache 2.0
// ============================================================================
#pragma once

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <print>
#include <thread>
#include <vector>

#include "asyncore.hpp"

namespace pooriayousefi::cpu_bound
{
    using namespace core;

    // ----------------------------------------------------------------
    //  Chase-Lev Lock-Free Work-Stealing Deque
    // ----------------------------------------------------------------

    /// @brief Lock-free work-stealing deque (Chase-Lev algorithm).
    ///
    /// Each worker has its own deque. Workers push/pop from the bottom
    /// (LIFO for cache locality). Other workers steal from the top
    /// (FIFO).  Uses a fixed-capacity circular buffer with atomic
    /// indices and careful memory ordering.
    class ChaseLevDeque
    {
    private:
        static constexpr std::size_t CAPACITY = 1024;

        std::atomic<int64_t> top_{0};
        std::atomic<int64_t> bottom_{0};
        std::array<std::atomic<std::coroutine_handle<>>, CAPACITY> buffer_{};

    public:
        ChaseLevDeque() = default;

        ChaseLevDeque(const ChaseLevDeque&) = delete;
        ChaseLevDeque& operator=(const ChaseLevDeque&) = delete;
        ChaseLevDeque(ChaseLevDeque&&) = delete;
        ChaseLevDeque& operator=(ChaseLevDeque&&) = delete;

        /// @brief Pushes a coroutine handle onto the bottom of the deque.
        /// @return true if pushed, false if the deque is full.
        bool push(std::coroutine_handle<> h) noexcept
        {
            bool result = false;

            int64_t b = bottom_.load(std::memory_order_relaxed);
            int64_t t = top_.load(std::memory_order_acquire);

            if (b - t < static_cast<int64_t>(CAPACITY))
            {
                buffer_[b % CAPACITY].store(h, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_release);
                bottom_.store(b + 1, std::memory_order_relaxed);
                result = true;
            }

            return result;
        }

        /// @brief Pops a coroutine handle from the bottom of the deque.
        /// @return the handle, or nullptr if empty.
        std::coroutine_handle<> pop() noexcept
        {
            std::coroutine_handle<> result{nullptr};

            int64_t b = bottom_.load(std::memory_order_relaxed) - 1;
            bottom_.store(b, std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_seq_cst);

            int64_t t = top_.load(std::memory_order_acquire);

            if (t <= b)
            {
                result = buffer_[b % CAPACITY].load(std::memory_order_relaxed);

                if (t == b)
                {
                    // Last element — race with stealers.
                    if (!top_.compare_exchange_strong(t, t + 1,
                                                      std::memory_order_seq_cst,
                                                      std::memory_order_relaxed))
                    {
                        result = nullptr;
                    }
                    bottom_.store(t + 1, std::memory_order_relaxed);
                }
            }
            else
            {
                // Deque was empty.
                bottom_.store(t, std::memory_order_relaxed);
                result = nullptr;
            }

            return result;
        }

        /// @brief Steals a coroutine handle from the top of the deque.
        /// @return the handle, or nullptr if empty.
        std::coroutine_handle<> steal() noexcept
        {
            std::coroutine_handle<> result{nullptr};

            int64_t t = top_.load(std::memory_order_acquire);
            int64_t b = bottom_.load(std::memory_order_acquire);

            if (t < b)
            {
                result = buffer_[t % CAPACITY].load(std::memory_order_relaxed);

                if (!top_.compare_exchange_strong(t, t + 1,
                                                  std::memory_order_seq_cst,
                                                  std::memory_order_relaxed))
                {
                    result = nullptr;
                }
            }

            return result;
        }
    };

    // ----------------------------------------------------------------
    //  CPU-Bound Thread Pool
    // ----------------------------------------------------------------

    /// @brief Coroutine-native thread pool for CPU-bound work.
    ///
    /// Each worker has a local Chase-Lev work-stealing deque for cache
    /// locality.  When a worker's local queue is empty, it tries the
    /// global queue, then steals from other workers.
    ///
    /// Uses DetachedTask (lazy start, auto-destroy) for safe coroutine
    /// lifecycle management — no dangling handles, no leaks.
    class ThreadPool
    {
    private:
        struct WorkerData
        {
            ChaseLevDeque local_queue{};
        };

        std::vector<std::unique_ptr<WorkerData>> workers_{};
        std::vector<std::thread> threads_{};
        std::mutex global_mtx_{};
        std::condition_variable global_cv_{};
        std::deque<std::coroutine_handle<>> global_queue_{};
        std::atomic<bool> stopped_{false};
        std::atomic<std::size_t> active_tasks_{0};
        std::atomic<std::size_t> total_queued_tasks_{0};

        inline static thread_local std::size_t tl_worker_id = 0;
        inline static thread_local bool tl_is_worker = false;

        void decrement_active() noexcept
        {
            if (active_tasks_.fetch_sub(1, std::memory_order_relaxed) == 1)
            {
                std::lock_guard<std::mutex> lk(global_mtx_);
                global_cv_.notify_all();
            }
        }

        void worker_loop(std::size_t id)
        {
            tl_worker_id = id;
            tl_is_worker = true;

            while (!stopped_.load(std::memory_order_relaxed))
            {
                std::coroutine_handle<> task{nullptr};

                // 1. Try local queue (LIFO — cache friendly).
                task = workers_[id]->local_queue.pop();
                if (task)
                {
                    total_queued_tasks_.fetch_sub(1, std::memory_order_relaxed);
                }

                // 2. Try global queue.
                if (!task)
                {
                    std::lock_guard<std::mutex> lk(global_mtx_);
                    if (!global_queue_.empty())
                    {
                        task = std::move(global_queue_.front());
                        global_queue_.pop_front();
                        total_queued_tasks_.fetch_sub(1, std::memory_order_relaxed);
                    }
                }

                // 3. Steal from other workers.
                if (!task)
                {
                    for (std::size_t i = 1; i < workers_.size() && !task; ++i)
                    {
                        std::size_t target = (id + i) % workers_.size();
                        task = workers_[target]->local_queue.steal();
                        if (task)
                        {
                            total_queued_tasks_.fetch_sub(1, std::memory_order_relaxed);
                        }
                    }
                }

                // 4. Execute or wait.
                if (task)
                {
                    task.resume();
                }
                else
                {
                    std::unique_lock<std::mutex> lk(global_mtx_);
                    global_cv_.wait(lk, [&]
                    {
                        return stopped_.load(std::memory_order_relaxed) ||
                               total_queued_tasks_.load(std::memory_order_relaxed) > 0;
                    });
                }
            }
        }

    public:
        explicit ThreadPool(std::size_t worker_count = 0)
        {
            if (worker_count == 0)
            {
                worker_count = std::thread::hardware_concurrency();
                if (worker_count == 0)
                {
                    worker_count = 2;
                }
            }

            workers_.reserve(worker_count);
            for (std::size_t id = 0; id < worker_count; ++id)
            {
                workers_.push_back(std::make_unique<WorkerData>());
            }

            threads_.reserve(worker_count);
            for (std::size_t id = 0; id < worker_count; ++id)
            {
                threads_.emplace_back([this, id]()
                {
                    worker_loop(id);
                });
            }
        }

        ~ThreadPool()
        {
            stopped_.store(true, std::memory_order_relaxed);
            global_cv_.notify_all();

            for (auto& t : threads_)
            {
                if (t.joinable())
                {
                    t.join();
                }
            }
        }

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;
        ThreadPool(ThreadPool&&) = delete;
        ThreadPool& operator=(ThreadPool&&) = delete;

        // ----------------------------------------------------------------
        //  enqueue_raw — push to local queue (if worker) or global queue
        // ----------------------------------------------------------------

        void enqueue_raw(std::coroutine_handle<> h)
        {
            total_queued_tasks_.fetch_add(1, std::memory_order_relaxed);

            if (tl_is_worker)
            {
                if (!workers_[tl_worker_id]->local_queue.push(h))
                {
                    // Local queue full — fall back to global.
                    std::lock_guard<std::mutex> lk(global_mtx_);
                    global_queue_.push_back(h);
                }
            }
            else
            {
                std::lock_guard<std::mutex> lk(global_mtx_);
                global_queue_.push_back(h);
            }

            global_cv_.notify_one();
        }

        // ----------------------------------------------------------------
        //  schedule_global — push to global queue (forces thread switch)
        // ----------------------------------------------------------------

        void schedule_global(std::coroutine_handle<> h)
        {
            total_queued_tasks_.fetch_add(1, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(global_mtx_);
                global_queue_.push_back(h);
            }
            global_cv_.notify_one();
        }

        // ----------------------------------------------------------------
        //  Schedule awaitable — co_await pool.schedule()
        // ----------------------------------------------------------------

        struct ScheduleAwaitable
        {
            ThreadPool* pool;

            bool await_ready() const noexcept
            {
                return false;
            }

            void await_suspend(std::coroutine_handle<> h) const
            {
                pool->schedule_global(h);
            }

            void await_resume() const noexcept
            {
            }
        };

        ScheduleAwaitable schedule() noexcept
        {
            return ScheduleAwaitable{this};
        }

        // ----------------------------------------------------------------
        //  run — execute an AsyncTask<T>, return std::future<T>
        // ----------------------------------------------------------------

        template <typename T>
        std::future<T> run(AsyncTask<T> t)
        {
            auto prom = std::make_shared<std::promise<T>>();
            auto fut = prom->get_future();

            active_tasks_.fetch_add(1, std::memory_order_relaxed);

            auto exec = [](ThreadPool* pool,
                           AsyncTask<T> t,
                           std::shared_ptr<std::promise<T>> prom) -> DetachedTask
            {
                try
                {
                    if constexpr (std::is_void_v<T>)
                    {
                        co_await t;
                        prom->set_value();
                    }
                    else
                    {
                        T val = co_await t;
                        prom->set_value(std::move(val));
                    }
                }
                catch (...)
                {
                    prom->set_exception(std::current_exception());
                }

                pool->decrement_active();
                co_return;
            };

            auto dt = exec(this, std::move(t), prom);
            auto h = dt.handle;
            dt.detach();
            enqueue_raw(h);

            return fut;
        }

        // ----------------------------------------------------------------
        //  spawn — fire-and-forget an AsyncTask<T>
        // ----------------------------------------------------------------

        template <typename T>
        void spawn(AsyncTask<T> t)
        {
            active_tasks_.fetch_add(1, std::memory_order_relaxed);

            auto exec = [](ThreadPool* pool, AsyncTask<T> t) -> DetachedTask
            {
                try
                {
                    co_await t;
                }
                catch (...)
                {
                    // Swallow — fire-and-forget.
                }

                pool->decrement_active();
                co_return;
            };

            auto dt = exec(this, std::move(t));
            auto h = dt.handle;
            dt.detach();
            enqueue_raw(h);
        }

        // ----------------------------------------------------------------
        //  submit — run a plain callable (non-coroutine), return future
        // ----------------------------------------------------------------

        template <typename F, typename... Args>
            requires std::invocable<F, Args...>
        [[nodiscard]] auto submit(F&& f, Args&&... args)
            -> std::future<std::invoke_result_t<F, Args...>>
        {
            using R = std::invoke_result_t<F, Args...>;

            auto prom = std::make_shared<std::promise<R>>();
            auto fut = prom->get_future();

            auto bound = [f = std::forward<F>(f),
                          args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable -> R
            {
                return std::apply(std::move(f), std::move(args_tuple));
            };

            active_tasks_.fetch_add(1, std::memory_order_relaxed);

            auto exec = [](ThreadPool* pool,
                           decltype(bound) bound,
                           std::shared_ptr<std::promise<R>> prom) -> DetachedTask
            {
                try
                {
                    if constexpr (std::is_void_v<R>)
                    {
                        bound();
                        prom->set_value();
                    }
                    else
                    {
                        prom->set_value(bound());
                    }
                }
                catch (...)
                {
                    prom->set_exception(std::current_exception());
                }

                pool->decrement_active();
                co_return;
            };

            auto dt = exec(this, std::move(bound), prom);
            auto h = dt.handle;
            dt.detach();
            enqueue_raw(h);

            return fut;
        }

        // ----------------------------------------------------------------
        //  wait — block until all tasks complete
        // ----------------------------------------------------------------

        void wait()
        {
            std::unique_lock<std::mutex> lk(global_mtx_);
            global_cv_.wait(lk, [&]
            {
                return active_tasks_.load(std::memory_order_relaxed) == 0 &&
                       total_queued_tasks_.load(std::memory_order_relaxed) == 0;
            });
        }

        [[nodiscard]] std::size_t worker_count() const noexcept
        {
            return threads_.size();
        }
    };

    // ----------------------------------------------------------------
    //  Structured Concurrency — TaskGroup
    // ----------------------------------------------------------------

    /// @brief Structured concurrency: spawn multiple tasks, wait for all.
    ///
    /// The first exception thrown by any spawned task is stored and
    /// rethrown when wait() is awaited.  Uses DetachedTask internally.
    class TaskGroup
    {
    private:
        std::atomic<std::size_t> active_count_{0};
        std::mutex exception_mtx_{};
        std::exception_ptr exception_{nullptr};
        std::atomic<std::coroutine_handle<>> waiter_{nullptr};
        ThreadPool* pool_;

        void store_exception(std::exception_ptr ptr)
        {
            std::lock_guard<std::mutex> lock(exception_mtx_);
            if (!exception_)
            {
                exception_ = ptr;
            }
        }

        void decrement_active() noexcept
        {
            if (active_count_.fetch_sub(1, std::memory_order_acq_rel) == 1)
            {
                auto w = waiter_.exchange(nullptr, std::memory_order_acq_rel);
                if (w)
                {
                    w.resume();
                }
            }
        }

    public:
        explicit TaskGroup(ThreadPool& pool) : pool_{&pool}
        {
        }

        ~TaskGroup() = default;

        TaskGroup(const TaskGroup&) = delete;
        TaskGroup& operator=(const TaskGroup&) = delete;
        TaskGroup(TaskGroup&&) = delete;
        TaskGroup& operator=(TaskGroup&&) = delete;

        // ----------------------------------------------------------------
        //  spawn — fire-and-forget an AsyncTask inside the group
        // ----------------------------------------------------------------

        template <typename T>
        void spawn(AsyncTask<T> t)
        {
            active_count_.fetch_add(1, std::memory_order_relaxed);

            auto exec = [](TaskGroup* group, AsyncTask<T> t) -> DetachedTask
            {
                try
                {
                    co_await t;
                }
                catch (...)
                {
                    group->store_exception(std::current_exception());
                }

                group->decrement_active();
                co_return;
            };

            auto dt = exec(this, std::move(t));
            auto h = dt.handle;
            dt.detach();
            pool_->enqueue_raw(h);
        }

        // ----------------------------------------------------------------
        //  wait — awaitable: suspend until all spawned tasks complete
        // ----------------------------------------------------------------

        struct WaitAwaitable
        {
            TaskGroup* group;

            bool await_ready() const noexcept
            {
                return group->active_count_.load(std::memory_order_acquire) == 0;
            }

            void await_suspend(std::coroutine_handle<> h) const
            {
                group->waiter_.store(h, std::memory_order_release);

                // Re-check after storing — avoids lost wakeup.
                if (group->active_count_.load(std::memory_order_acquire) == 0)
                {
                    if (auto w = group->waiter_.exchange(nullptr,
                                                          std::memory_order_acq_rel))
                    {
                        w.resume();
                    }
                }
            }

            void await_resume()
            {
                std::exception_ptr e;
                {
                    std::lock_guard<std::mutex> lock(group->exception_mtx_);
                    e = group->exception_;
                }
                if (e)
                {
                    std::rethrow_exception(e);
                }
            }
        };

        WaitAwaitable wait() noexcept
        {
            return WaitAwaitable{this};
        }
    };
}