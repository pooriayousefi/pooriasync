// ============================================================================
//  io_thread_pool.hpp — Cross-Platform Async IO Thread Pool & Reactor
//  Developed by: Pooria Yousefi
//  License: Apache 2.0
// ============================================================================
#pragma once

#include <algorithm>
#include <atomic>
#include <mutex>
#include <deque>
#include <unordered_map>
#include <thread>
#include <memory>
#include <vector>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <future>
#include <expected>
#include <span>
#include <system_error>

#include "asyncore.hpp"

// Cross-platform OS headers
#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#include <unistd.h>
#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#elif defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#pragma comment(lib, "ws2_32.lib")
#endif

// Cross-platform socket abstraction
#if defined(_WIN32)
using socket_t = SOCKET;
#define INVALID_SOCK INVALID_SOCKET
#define SOCK_ERR SOCKET_ERROR
inline void close_socket_impl(socket_t s) { closesocket(s); }
inline bool set_nonblocking(socket_t s)
{
    u_long mode = 1;
    return ioctlsocket(s, FIONBIO, &mode) == 0;
}
inline int get_socket_error() { return WSAGetLastError(); }
constexpr int SOCKET_EWOULDBLOCK = WSAEWOULDBLOCK;
constexpr int SOCKET_EINPROGRESS = WSAEWOULDBLOCK;
struct WSAInit
{
    WSAInit()
    {
        WSADATA wsaData;
        WSAStartup(MAKEWORD(2, 2), &wsaData);
    }
    ~WSAInit() { WSACleanup(); }
};
static WSAInit wsa_init;
#else
using socket_t = int;
#define INVALID_SOCK -1
#define SOCK_ERR (-1)
inline void close_socket_impl(socket_t s) { ::close(s); }
inline bool set_nonblocking(socket_t s) { return fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK) != -1; }
inline int get_socket_error() { return errno; }
constexpr int SOCKET_EWOULDBLOCK = EWOULDBLOCK;
constexpr int SOCKET_EINPROGRESS = EINPROGRESS;
#endif

namespace pooriayousefi::io_bound
{
    using namespace core;

    inline thread_local class NetworkReactor *current_reactor = nullptr;

    class NetworkReactor
    {
    public:
        enum class EventType
        {
            READABLE,
            WRITABLE
        };

        struct ScheduleAwaitable
        {
            NetworkReactor* reactor;
            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<> h) const
            {
                reactor->schedule([h]() { h.resume(); });
            }
            void await_resume() const noexcept {}
        };

        ScheduleAwaitable yield() noexcept { return ScheduleAwaitable{this}; }

    private:
        std::deque<MoveOnlyFunction> queue_;
        std::mutex mtx_;
        std::atomic<bool> stopped_{false};

        std::unordered_map<socket_t, MoveOnlyFunction> read_cbs_;
        std::unordered_map<socket_t, MoveOnlyFunction> write_cbs_;

#if defined(__linux__)
        int epoll_fd_;
        int event_fd_;
#elif defined(__APPLE__) || defined(__FreeBSD__)
        int kqueue_fd_;
        int wake_pipe_[2];
#elif defined(_WIN32)
        SOCKET wake_socket_;
#endif

    public:
        NetworkReactor()
        {
#if defined(__linux__)
            epoll_fd_ = epoll_create1(0);
            event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            epoll_event event{};
            event.events = EPOLLIN;
            event.data.fd = event_fd_;
            epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &event);
#elif defined(__APPLE__) || defined(__FreeBSD__)
            kqueue_fd_ = kqueue();
            pipe(wake_pipe_);
            set_nonblocking(wake_pipe_[0]);
            set_nonblocking(wake_pipe_[1]);
            struct kevent ev;
            EV_SET(&ev, wake_pipe_[0], EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
            kevent(kqueue_fd_, &ev, 1, nullptr, 0, nullptr);
#elif defined(_WIN32)
            wake_socket_ = INVALID_SOCKET;
#endif
        }

        ~NetworkReactor()
        {
            stop();
#if defined(__linux__)
            close(event_fd_);
            close(epoll_fd_);
#elif defined(__APPLE__) || defined(__FreeBSD__)
            close(wake_pipe_[0]);
            close(wake_pipe_[1]);
            close(kqueue_fd_);
#endif
        }

        void schedule(MoveOnlyFunction AsyncTask)
        {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                queue_.push_back(std::move(AsyncTask));
            }
            wake_up();
        }

        void stop()
        {
            stopped_.store(true, std::memory_order_release);
            wake_up();
        }

        void wake_up()
        {
#if defined(__linux__)
            uint64_t val = 1;
            write(event_fd_, &val, sizeof(val));
#elif defined(__APPLE__) || defined(__FreeBSD__)
            char val = '1';
            write(wake_pipe_[1], &val, 1);
#endif
        }

        void register_socket(socket_t fd, EventType type, MoveOnlyFunction cb)
        {
            std::lock_guard<std::mutex> lock(mtx_);
#if defined(__linux__)
            if (type == EventType::READABLE)
                read_cbs_[fd] = std::move(cb);
            else
                write_cbs_[fd] = std::move(cb);

            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = (read_cbs_.count(fd) ? EPOLLIN : 0) | (write_cbs_.count(fd) ? EPOLLOUT : 0);
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == -1)
            {
                if (errno == ENOENT)
                    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
            }
#elif defined(__APPLE__) || defined(__FreeBSD__)
            if (type == EventType::READABLE)
                read_cbs_[fd] = std::move(cb);
            else
                write_cbs_[fd] = std::move(cb);

            struct kevent changes[2];
            int n = 0;
            if (read_cbs_.count(fd))
                EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
            if (write_cbs_.count(fd))
                EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, nullptr);
            kevent(kqueue_fd_, changes, n, nullptr, 0, nullptr);
#elif defined(_WIN32)
            if (type == EventType::READABLE)
                read_cbs_[fd] = std::move(cb);
            else
                write_cbs_[fd] = std::move(cb);
#endif
        }

        void deregister_socket(socket_t fd, EventType type)
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (type == EventType::READABLE)
                read_cbs_.erase(fd);
            else
                write_cbs_.erase(fd);

#if defined(__linux__)
            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = (read_cbs_.count(fd) ? EPOLLIN : 0) | (write_cbs_.count(fd) ? EPOLLOUT : 0);
            if (ev.events == 0)
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            else
                epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
#elif defined(__APPLE__) || defined(__FreeBSD__)
            struct kevent change;
            int filter = (type == EventType::READABLE) ? EVFILT_READ : EVFILT_WRITE;
            EV_SET(&change, fd, filter, EV_DELETE, 0, 0, nullptr);
            kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr);
#endif
        }

        void run()
        {
            current_reactor = this;
            while (!stopped_.load(std::memory_order_acquire))
            {
#if defined(__linux__)
                epoll_event events[64];
                int n = epoll_wait(epoll_fd_, events, 64, 100);
                if (n == -1 && errno == EINTR)
                    continue;
                for (int i = 0; i < n; ++i)
                {
                    if (events[i].data.fd == event_fd_)
                    {
                        uint64_t val;
                        read(event_fd_, &val, sizeof(val));
                    }
                    else
                    {
                        socket_t fd = events[i].data.fd;
                        MoveOnlyFunction r_cb, w_cb;
                        {
                            std::lock_guard<std::mutex> lock(mtx_);
                            if (events[i].events & EPOLLIN)
                            {
                                auto it = read_cbs_.find(fd);
                                if (it != read_cbs_.end())
                                    r_cb = std::move(it->second);
                                read_cbs_.erase(fd);
                            }
                            if (events[i].events & EPOLLOUT)
                            {
                                auto it = write_cbs_.find(fd);
                                if (it != write_cbs_.end())
                                    w_cb = std::move(it->second);
                                write_cbs_.erase(fd);
                            }
                        }
                        if (r_cb)
                            r_cb();
                        if (w_cb)
                            w_cb();
                    }
                }
#elif defined(__APPLE__) || defined(__FreeBSD__)
                struct kevent events[64];
                int n = kevent(kqueue_fd_, nullptr, 0, events, 64, nullptr);
                if (n == -1 && errno == EINTR)
                    continue;
                for (int i = 0; i < n; ++i)
                {
                    if (events[i].ident == wake_pipe_[0])
                    {
                        char buf[16];
                        read(wake_pipe_[0], buf, sizeof(buf));
                    }
                    else
                    {
                        socket_t fd = (socket_t)events[i].ident;
                        MoveOnlyFunction cb;
                        {
                            std::lock_guard<std::mutex> lock(mtx_);
                            if (events[i].filter == EVFILT_READ)
                            {
                                auto it = read_cbs_.find(fd);
                                if (it != read_cbs_.end())
                                    cb = std::move(it->second);
                                read_cbs_.erase(fd);
                            }
                            else if (events[i].filter == EVFILT_WRITE)
                            {
                                auto it = write_cbs_.find(fd);
                                if (it != write_cbs_.end())
                                    cb = std::move(it->second);
                                write_cbs_.erase(fd);
                            }
                        }
                        if (cb)
                            cb();
                    }
                }
#elif defined(_WIN32)
                fd_set read_fds, write_fds;
                FD_ZERO(&read_fds);
                FD_ZERO(&write_fds);

                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    for (auto &pair : read_cbs_)
                        FD_SET(pair.first, &read_fds);
                    for (auto &pair : write_cbs_)
                        FD_SET(pair.first, &write_fds);
                }

                struct timeval tv;
                tv.tv_sec = 0;
                tv.tv_usec = 100000; // 100ms

                int n = select(0, &read_fds, &write_fds, nullptr, &tv);
                if (n > 0)
                {
                    std::vector<socket_t> ready_reads, ready_writes;
                    {
                        std::lock_guard<std::mutex> lock(mtx_);
                        for (auto &pair : read_cbs_)
                            if (FD_ISSET(pair.first, &read_fds))
                                ready_reads.push_back(pair.first);
                        for (auto &pair : write_cbs_)
                            if (FD_ISSET(pair.first, &write_fds))
                                ready_writes.push_back(pair.first);
                    }
                    for (socket_t fd : ready_reads)
                    {
                        MoveOnlyFunction cb;
                        {
                            std::lock_guard<std::mutex> lock(mtx_);
                            auto it = read_cbs_.find(fd);
                            if (it != read_cbs_.end())
                                cb = std::move(it->second);
                            read_cbs_.erase(fd);
                        }
                        if (cb)
                            cb();
                    }
                    for (socket_t fd : ready_writes)
                    {
                        MoveOnlyFunction cb;
                        {
                            std::lock_guard<std::mutex> lock(mtx_);
                            auto it = write_cbs_.find(fd);
                            if (it != write_cbs_.end())
                                cb = std::move(it->second);
                            write_cbs_.erase(fd);
                        }
                        if (cb)
                            cb();
                    }
                }
#endif
                std::deque<MoveOnlyFunction> local;
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    local.swap(queue_);
                }
                while (!local.empty())
                {
                    try
                    {
                        local.front()();
                    }
                    catch (...)
                    {
                    }
                    local.pop_front();
                }
            }
        }
    };

    // Enterprise Enhancement: Async TCP Socket with std::expected and std::span
    class AsyncSocket
    {
        socket_t fd_{INVALID_SOCK};
        NetworkReactor *reactor_{nullptr};

    public:
        AsyncSocket() : reactor_(current_reactor) {}
        ~AsyncSocket() { close(); }

        AsyncSocket(const AsyncSocket &) = delete;
        AsyncSocket &operator=(const AsyncSocket &) = delete;

        AsyncSocket(AsyncSocket &&other) noexcept 
            : fd_(std::exchange(other.fd_, INVALID_SOCK))
            , reactor_(other.reactor_) 
        {}

        AsyncSocket &operator=(AsyncSocket &&other) noexcept
        {
            if (this != &other)
            {
                close();
                fd_ = std::exchange(other.fd_, INVALID_SOCK);
                reactor_ = other.reactor_;
            }
            return *this;
        }

        bool is_open() const { return fd_ != INVALID_SOCK; }
        socket_t native_handle() const { return fd_; }

        void assign(socket_t fd)
        {
            close();
            fd_ = fd;
            if (fd_ != INVALID_SOCK)
                set_nonblocking(fd_);
        }

        void close()
        {
            if (fd_ != INVALID_SOCK)
            {
                if (reactor_)
                    reactor_->deregister_socket(fd_, NetworkReactor::EventType::READABLE);
                if (reactor_)
                    reactor_->deregister_socket(fd_, NetworkReactor::EventType::WRITABLE);
                close_socket_impl(fd_);
                fd_ = INVALID_SOCK;
            }
        }

        struct Awaitable
        {
            NetworkReactor *reactor;
            socket_t fd;
            NetworkReactor::EventType type;
            bool await_ready() { return false; }
            void await_suspend(std::coroutine_handle<> h)
            {
                reactor->register_socket(
                    fd, 
                    type, 
                    [h]()
                    { 
                        h.resume(); 
                    }
                );
            }
            void await_resume() {}
        };

        AsyncTask<std::expected<void, std::error_code>> async_connect(const std::string &host, int port)
        {
            if (!reactor_)
            {
                co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
            }

            struct addrinfo hints = {};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            struct addrinfo *result = nullptr;
            if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0 || !result)
            {
                co_return std::unexpected(std::make_error_code(std::errc::address_not_available));
            }

            fd_ = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
            if (fd_ == INVALID_SOCK)
            {
                freeaddrinfo(result);
                co_return std::unexpected(std::make_error_code(std::errc::address_not_available));
            }

            set_nonblocking(fd_);
            if (::connect(fd_, result->ai_addr, (int)result->ai_addrlen) == 0)
            {
                freeaddrinfo(result);
                co_return {};
            }

            int err = get_socket_error();
            if (err != SOCKET_EINPROGRESS && err != SOCKET_EWOULDBLOCK)
            {
                close();
                freeaddrinfo(result);
                co_return std::unexpected(std::make_error_code(static_cast<std::errc>(err)));
            }

            freeaddrinfo(result);
            co_await Awaitable{reactor_, fd_, NetworkReactor::EventType::WRITABLE};

            int error = 0;
            socklen_t len = sizeof(error);
            getsockopt(fd_, SOL_SOCKET, SO_ERROR, (char *)&error, &len);
            if (error != 0)
            {
                close();
                co_return std::unexpected(std::make_error_code(static_cast<std::errc>(error)));
            }
            co_return {};
        }

        AsyncTask<std::expected<size_t, std::error_code>> send(std::span<const std::byte> buf)
        {
            if (!reactor_)
            {
                co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
            }
            size_t total_sent = 0;
            while (total_sent < buf.size())
            {
                int n = ::send(fd_, reinterpret_cast<const char*>(buf.data() + total_sent), static_cast<int>(buf.size() - total_sent), 0);
                if (n == SOCK_ERR)
                {
                    if (get_socket_error() != SOCKET_EWOULDBLOCK)
                        co_return std::unexpected(std::make_error_code(static_cast<std::errc>(get_socket_error())));
                    co_await Awaitable{reactor_, fd_, NetworkReactor::EventType::WRITABLE};
                    continue;
                }
                total_sent += n;
            }
            co_return total_sent;
        }

        AsyncTask<std::expected<size_t, std::error_code>> recv(std::span<std::byte> buf)
        {
            if (!reactor_)
            {
                co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
            }
            while (true)
            {
                int n = ::recv(fd_, reinterpret_cast<char*>(buf.data()), static_cast<int>(buf.size()), 0);
                if (n == SOCK_ERR)
                {
                    if (get_socket_error() != SOCKET_EWOULDBLOCK)
                        co_return std::unexpected(std::make_error_code(static_cast<std::errc>(get_socket_error())));
                    co_await Awaitable{reactor_, fd_, NetworkReactor::EventType::READABLE};
                    continue;
                }
                co_return static_cast<size_t>(n);
            }
        }
    };

    // Enterprise Enhancement: AsyncPipe for IPC/stdio
    class AsyncPipe
    {
    private:
        socket_t read_fd_{INVALID_SOCK};
        socket_t write_fd_{INVALID_SOCK};
        NetworkReactor *reactor_{nullptr};

    public:
        AsyncPipe() : reactor_(current_reactor) {}
        ~AsyncPipe() { close(); }

        AsyncPipe(const AsyncPipe &) = delete;
        AsyncPipe &operator=(const AsyncPipe &) = delete;

        // FIX: Properly defined move constructors to allow std::optional<AsyncPipe> to be movable
        AsyncPipe(AsyncPipe &&other) noexcept
            : read_fd_(std::exchange(other.read_fd_, INVALID_SOCK))
            , write_fd_(std::exchange(other.write_fd_, INVALID_SOCK))
            , reactor_(other.reactor_)
        {}

        AsyncPipe &operator=(AsyncPipe &&other) noexcept
        {
            if (this != &other)
            {
                close();
                read_fd_ = std::exchange(other.read_fd_, INVALID_SOCK);
                write_fd_ = std::exchange(other.write_fd_, INVALID_SOCK);
                reactor_ = other.reactor_;
            }
            return *this;
        }

        void assign_write(socket_t fd)
        {
            write_fd_ = fd;
#ifndef _WIN32
            if (write_fd_ != INVALID_SOCK)
                set_nonblocking(write_fd_);
#endif
        }

        void assign_read(socket_t fd)
        {
            read_fd_ = fd;
#ifndef _WIN32
            if (read_fd_ != INVALID_SOCK)
                set_nonblocking(read_fd_);
#endif
        }

        void close()
        {
            if (reactor_)
            {
                if (read_fd_ != INVALID_SOCK)
                    reactor_->deregister_socket(read_fd_, NetworkReactor::EventType::READABLE);
                if (write_fd_ != INVALID_SOCK)
                    reactor_->deregister_socket(write_fd_, NetworkReactor::EventType::WRITABLE);
            }
            // Note: Process class owns the actual OS handles and will close them.
            read_fd_ = INVALID_SOCK;
            write_fd_ = INVALID_SOCK;
        }

        struct Awaitable
        {
            NetworkReactor *reactor;
            socket_t fd;
            NetworkReactor::EventType type;
            bool await_ready() { return false; }
            void await_suspend(std::coroutine_handle<> h)
            {
                reactor->register_socket(
                    fd, 
                    type, 
                    [h]()
                    { 
                        h.resume(); 
                    }
                );
            }
            void await_resume() {}
        };

        AsyncTask<std::expected<size_t, std::error_code>> send(std::span<const std::byte> buf)
        {
            if (!reactor_)
            {
                co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
            }

            size_t total_sent = 0;
            while (total_sent < buf.size())
            {
#if defined(_WIN32)
                // Windows anonymous pipes do not support non-blocking writes easily.
                // We block here. For high-performance Windows IPC, IOCP is required.
                DWORD bytes_written = 0;
                BOOL success = WriteFile(
                    reinterpret_cast<HANDLE>(write_fd_),
                    buf.data() + total_sent,
                    static_cast<DWORD>(buf.size() - total_sent),
                    &bytes_written,
                    nullptr
                );
                if (!success)
                {
                    co_return std::unexpected(std::make_error_code(std::errc::io_error));
                }
                total_sent += bytes_written;
#else
                ssize_t n = ::write(write_fd_, buf.data() + total_sent, buf.size() - total_sent);
                if (n < 0)
                {
                    if (errno != EWOULDBLOCK && errno != EAGAIN)
                        co_return std::unexpected(std::make_error_code(static_cast<std::errc>(errno)));
                    co_await Awaitable{reactor_, write_fd_, NetworkReactor::EventType::WRITABLE};
                    continue;
                }
                total_sent += n;
#endif
            }
            co_return total_sent;
        }

        AsyncTask<std::expected<size_t, std::error_code>> recv(std::span<std::byte> buf)
        {
            if (!reactor_)
            {
                co_return std::unexpected(std::make_error_code(std::errc::operation_not_permitted));
            }

#if defined(_WIN32)
            // Windows pipe workaround: Use PeekNamedPipe to poll for data without blocking,
            // yielding to the reactor when no data is available.
            HANDLE hRead = reinterpret_cast<HANDLE>(read_fd_);
            while (true)
            {
                DWORD bytes_available = 0;
                BOOL success = PeekNamedPipe(
                    hRead,
                    nullptr,
                    0,
                    nullptr,
                    &bytes_available,
                    nullptr
                );

                if (!success)
                {
                    co_return std::unexpected(std::make_error_code(std::errc::broken_pipe));
                }

                if (bytes_available > 0)
                {
                    DWORD to_read = static_cast<DWORD>(std::min<DWORD>(bytes_available, static_cast<DWORD>(buf.size())));
                    DWORD bytes_read = 0;
                    success = ReadFile(
                        hRead,
                        buf.data(),
                        to_read,
                        &bytes_read,
                        nullptr
                    );

                    if (success)
                    {
                        co_return static_cast<size_t>(bytes_read);
                    }
                    co_return std::unexpected(std::make_error_code(std::errc::io_error));
                }

                // No data available, yield execution to the reactor
                co_await reactor_->yield();
            }
#else
            while (true)
            {
                ssize_t n = ::read(read_fd_, buf.data(), buf.size());
                if (n < 0)
                {
                    if (errno != EWOULDBLOCK && errno != EAGAIN)
                        co_return std::unexpected(std::make_error_code(static_cast<std::errc>(errno)));
                    co_await Awaitable{reactor_, read_fd_, NetworkReactor::EventType::READABLE};
                    continue;
                }
                co_return static_cast<size_t>(n);
            }
#endif
        }
    };

    class ThreadPool
    {
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
            worker_reactors_.reserve(worker_count);

            for (std::size_t id = 0; id < worker_count; ++id)
            {
                worker_reactors_.push_back(std::make_unique<NetworkReactor>());
            }

            for (std::size_t id = 0; id < worker_count; ++id)
            {
                workers_.emplace_back(
                    [this, id]
                    { 
                        worker_reactors_[id]->run(); 
                    }
                );
            }
        }

        ~ThreadPool()
        {
            for (auto &r : worker_reactors_)
            {
                if (r)
                {
                    r->stop();
                }
            }
            for (auto &w : workers_)
            {
                if (w.joinable())
                {
                    w.join();
                }
            }
        }

        ThreadPool(ThreadPool const &) = delete;
        ThreadPool &operator=(ThreadPool const &) = delete;

        template <typename F, typename... Args>
            requires std::invocable<F, Args...>
        [[nodiscard]] auto submit(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>>
        {
            using R = std::invoke_result_t<F, Args...>;
            auto prom = std::make_shared<std::promise<R>>();
            auto fut = prom->get_future();
            auto bound = [f = std::forward<F>(f), args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable -> R
            {
                return std::apply(std::move(f), std::move(args_tuple));
            };
            enqueue_raw(
                [prom, bound = std::move(bound)]() mutable
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
                }
            );
            return fut;
        }

        struct ScheduleAwaitable
        {
            ThreadPool *pool;
            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<> h) const
            {
                pool->enqueue_raw([h]() { h.resume(); });
            }
            void await_resume() const noexcept {}
        };

        ScheduleAwaitable schedule() noexcept { return ScheduleAwaitable{this}; }

        template <typename T>
        std::future<T> run(AsyncTask<T> t)
        {
            auto prom = std::make_shared<std::promise<T>>();
            auto fut = prom->get_future();
            auto exec = [](AsyncTask<T> t, std::shared_ptr<std::promise<T>> prom) -> FireAndForget
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
            };
            auto ff = exec(std::move(t), prom);
            auto h = ff.handle;
            ff.detach();
            enqueue_raw(
                [h]()
                { 
                    h.resume(); 
                }
            );
            return fut;
        }

        void enqueue_raw(MoveOnlyFunction task)
        {
            std::size_t target = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
            worker_reactors_[target]->schedule(std::move(task));
        }

        std::size_t worker_count() const noexcept { return workers_.size(); }

    private:
        std::vector<std::thread> workers_;
        std::vector<std::unique_ptr<NetworkReactor>> worker_reactors_;
        std::atomic<std::size_t> next_worker_{0};
    };
}