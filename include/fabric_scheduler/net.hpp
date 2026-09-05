#pragma once
// Minimal Winsock TCP transport used by the reference distributed coordinator
// and workers. Frames are delivered via FrameCodec; send_all handles partial
// writes, recv_frame handles partial reads and bounded payloads.
#include "fabric_scheduler/protocol.hpp"
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace fabric {

// RAII Winsock startup/shutdown (idempotent-ish). A no-op on non-Windows.
struct WinsockInit {
    WinsockInit();
    ~WinsockInit();
    WinsockInit(const WinsockInit&) = delete;
    WinsockInit& operator=(const WinsockInit&) = delete;
};

class TcpChannel {
public:
    TcpChannel() = default;
    explicit TcpChannel(std::uintptr_t socket) : socket_(socket) {}
    TcpChannel(TcpChannel&& other) noexcept;
    TcpChannel& operator=(TcpChannel&& other) noexcept;
    ~TcpChannel();
    TcpChannel(const TcpChannel&) = delete;
    TcpChannel& operator=(const TcpChannel&) = delete;

    static std::optional<TcpChannel> connect(const std::string& host, std::uint16_t port);

    bool valid() const noexcept { return socket_ != 0; }
    void close();

    // Blocking send of an entire frame (handles partial writes).
    void send_frame(MessageType type, const std::vector<std::byte>& payload,
                    std::uint8_t flags = 0);
    // Blocking receive of one validated frame (handles partial reads).
    std::pair<MessageType, std::vector<std::byte>> recv_frame();
    // Raw send/recv (for control messages), handling partial I/O.
    void send_all(const std::byte* data, std::size_t n);
    bool recv_exact(std::byte* data, std::size_t n);

private:
    std::uintptr_t socket_{0};
};

// A server listener bound to a loopback port.
class TcpListener {
public:
    TcpListener() = default;
    ~TcpListener();
    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;

    // Bind + listen on the given port (0 = auto-assign). Returns the bound port.
    std::uint16_t listen(std::uint16_t port);
    std::optional<TcpChannel> accept();

private:
    std::uintptr_t listener_{0};
};

}  // namespace fabric
