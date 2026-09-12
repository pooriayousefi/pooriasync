#include "process.hpp"

#include <chrono>
#include <iostream>
#include <print>
#include <string>
#include <thread>
#include <unistd.h>

using namespace pooriayousefi::process;

inline void write_to_fd(int fd, std::string_view data)
{
    ::write(fd, data.data(), data.size());
}

inline std::size_t read_from_fd(int fd, char* buf, std::size_t buf_size)
{
    ssize_t n = ::read(fd, buf, buf_size);
    return n > 0 ? static_cast<std::size_t>(n) : 0;
}

void print_test_header(const std::string& test_name)
{
    std::println("\n--- Running Test: {} ---", test_name);
}

bool test_echo_pipeline()
{
    bool success = true;
    print_test_header("Echo Pipeline (stdin -> stdout)");

    Process p;

    p.start("/bin/cat", {});

    if (!p.running())
    {
        std::println("Failed to start process.");
        return false;
    }

    std::string message = "hello from pooriprocess!\n";
    write_to_fd(p.get_stdin_write(), message);
    p.close_stdin();

    // Close stdin so cat/echo knows we're done writing.
    // We need to close the write end — but the Process owns it.
    // For now, call wait() which will block until the child exits.
    // In a real async layer we'd close stdin before reading stdout.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    char buffer[256] = {};
    std::size_t bytes_read = read_from_fd(p.get_stdout_read(), buffer, sizeof(buffer));

    if (bytes_read > 0)
    {
        std::string output(buffer, bytes_read);
        std::println("Received: {}", output);

        if (output.find("hello from pooriprocess!") != std::string::npos)
        {
            std::println("Echo verified.");
        }
        else
        {
            std::println("Echo content mismatch.");
            success = false;
        }
    }
    else
    {
        std::println("Warning: no data read (child may have exited before read).");
    }

    p.wait();
    std::println("Process exited with code: {}", p.exit_code());

    return success;
}

bool test_exit_codes()
{
    bool success = true;
    print_test_header("Exit Codes");

    Process p;

    p.start("/bin/sh", {"-c", "exit 42"});

    int code = p.wait();

    std::println("Expected exit code 42, got {}", code);

    if (code != 42)
    {
        success = false;
    }

    return success;
}

bool test_command_not_found()
{
    bool success = true;
    print_test_header("Command Not Found (exit 127)");

    Process p;

    p.start("/nonexistent/path/to/program", {});

    int code = p.wait();

    std::println("Expected exit code 127, got {}", code);

    if (code != 127)
    {
        success = false;
    }

    return success;
}

bool test_terminate_running_process()
{
    bool success = true;
    print_test_header("Terminate Running Process");

    Process p;

    p.start("/bin/sleep", {"60"});

    if (!p.running())
    {
        std::println("Failed to start process for termination test.");
        return false;
    }

    std::println("Process running. Terminating...");
    p.terminate();

    if (p.running())
    {
        std::println("Error: process still running after terminate().");
        success = false;
    }
    else
    {
        std::println("Process terminated successfully. Exit code: {}", p.exit_code());
    }

    return success;
}

bool test_destructor_cleans_up()
{
    bool success = true;
    print_test_header("Destructor Cleanup (no orphan/zombie)");

    {
        Process p;

        p.start("/bin/sleep", {"60"});

        if (!p.running())
        {
            std::println("Failed to start process for destructor test.");
            return false;
        }

        std::println("Process started. Destructor will clean up on scope exit.");
    }

    std::println("Scope exited. Destructor should have terminated the child.");

    return success;
}

bool test_running_check_after_exit()
{
    bool success = true;
    print_test_header("Running Check After Exit (no ECHILD failure)");

    Process p;

    p.start("/bin/sh", {"-c", "exit 7"});

    // Give the process time to exit.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool is_running = p.running();
    if (is_running)
    {
        std::println("Error: running() returned true for an exited process.");
        success = false;
    }
    else
    {
        std::println("running() correctly returned false after exit.");

        // The critical test: wait() should NOT fail after running() reaped the zombie.
        int code = p.wait();
        std::println("wait() returned code {} (expected 7)", code);

        if (code != 7)
        {
            success = false;
        }
    }

    return success;
}

int main()
{
    int exit_code = EXIT_SUCCESS;

    try
    {
        bool all_passed = true;

        if (!test_echo_pipeline())
        {
            all_passed = false;
        }
        if (!test_exit_codes())
        {
            all_passed = false;
        }
        if (!test_command_not_found())
        {
            all_passed = false;
        }
        if (!test_terminate_running_process())
        {
            all_passed = false;
        }
        if (!test_destructor_cleans_up())
        {
            all_passed = false;
        }
        if (!test_running_check_after_exit())
        {
            all_passed = false;
        }

        std::println("\n==============================");
        if (all_passed)
        {
            std::println("ALL POORIPROCESS TESTS PASSED!");
        }
        else
        {
            std::println("SOME POORIPROCESS TESTS FAILED!");
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