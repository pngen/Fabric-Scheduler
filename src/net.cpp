#include "fabric_scheduler/net.hpp"
#include "fabric_scheduler/core/error.hpp"
#include <winsock2.h>
#include <ws2tcpip.h>
#include <cstring>
#include <array>
#include <utility>

namespace fabric {

namespace {
bool winsockStarted = false;
int winsockRefs = 0;

SOCKET to_socket(std::uintptr_t s) { return static_cast<SOCKET>(s); }
std::uintptr_t from_socket(SOCKET s) { return static_cast<std::uintptr_t>(s); }
}  // namespace

WinsockInit::WinsockInit() {
    if (winsockRefs == 0) {
        WSADATA data;
        if (WSAStartup(MAKEWORD(2, 2), &data) != 0) throw FabricError(ErrorCode::ProtocolError, "WSAStartup failed");
        winsockStarted = true;
    }
    ++winsockRefs;
}

WinsockInit::~WinsockInit() {
    if (winsockRefs > 0) {
        --winsockRefs;
        if (winsockRefs == 0 && winsockStarted) { WSACleanup(); winsockStarted = false; }
    }
}

TcpChannel::~TcpChannel() { close(); }
TcpChannel::TcpChannel(TcpChannel&& other) noexcept : socket_(other.socket_) { other.socket_ = 0; }
TcpChannel& TcpChannel::operator=(TcpChannel&& other) noexcept {
    if (this != &other) { close(); socket_ = other.socket_; other.socket_ = 0; }
    return *this;
}

void TcpChannel::close() {
    if (socket_ != 0) { closesocket(to_socket(socket_)); socket_ = 0; }
}

std::optional<TcpChannel> TcpChannel::connect(const std::string& host, std::uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return std::nullopt;
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (::connect(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        closesocket(s);
        return std::nullopt;
    }
    return TcpChannel(from_socket(s));
}

void TcpChannel::send_all(const std::byte* data, std::size_t n) {
    if (socket_ == 0) throw FabricError(ErrorCode::ProtocolError, "send on closed socket");
    std::size_t sent = 0;
    while (sent < n) {
        const int chunk = ::send(to_socket(socket_), reinterpret_cast<const char*>(data + sent),
                              static_cast<int>(n - sent), 0);
        if (chunk == SOCKET_ERROR) throw FabricError(ErrorCode::ProtocolError, "send failed");
        if (chunk == 0) throw FabricError(ErrorCode::ProtocolError, "send returned 0");
        sent += static_cast<std::size_t>(chunk);
    }
}

bool TcpChannel::recv_exact(std::byte* data, std::size_t n) {
    if (socket_ == 0) throw FabricError(ErrorCode::ProtocolError, "recv on closed socket");
    std::size_t got = 0;
    while (got < n) {
        const int chunk = ::recv(to_socket(socket_), reinterpret_cast<char*>(data + got),
                              static_cast<int>(n - got), 0);
        if (chunk == 0) return false;
        if (chunk == SOCKET_ERROR) throw FabricError(ErrorCode::ProtocolError, "recv failed");
        got += static_cast<std::size_t>(chunk);
    }
    return true;
}

void TcpChannel::send_frame(MessageType type, const std::vector<std::byte>& payload, std::uint8_t flags) {
    const auto frame = FrameCodec::encode_frame(type, payload, flags);
    send_all(frame.data(), frame.size());
}

std::pair<MessageType, std::vector<std::byte>> TcpChannel::recv_frame() {
    std::array<std::byte, kFrameHeaderSize> hdr;
    if (!recv_exact(hdr.data(), hdr.size())) throw FabricError(ErrorCode::ProtocolError, "connection closed during header");
    const FrameHeader h = FrameCodec::parse_header(hdr.data());
    std::vector<std::byte> payload(h.payloadLen);
    if (h.payloadLen > 0 && !recv_exact(payload.data(), payload.size()))
        throw FabricError(ErrorCode::Truncation, "payload truncated");
    std::array<std::byte, 8> digest;
    if (!recv_exact(digest.data(), digest.size())) throw FabricError(ErrorCode::Truncation, "digest truncated");
    FrameCodec::check_digest(hdr.data(), payload.data(), h.payloadLen, digest.data());
    return { h.type, std::move(payload) };
}

TcpListener::~TcpListener() { if (listener_ != 0) { closesocket(to_socket(listener_)); listener_ = 0; } }

std::uint16_t TcpListener::listen(std::uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) throw FabricError(ErrorCode::ProtocolError, "socket() failed");
    BOOL reuse = TRUE;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        closesocket(s);
        throw FabricError(ErrorCode::ProtocolError, "bind() failed");
    }
    if (::listen(s, 16) == SOCKET_ERROR) { closesocket(s); throw FabricError(ErrorCode::ProtocolError, "listen() failed"); }
    if (port == 0) {
        struct sockaddr_in bound;
        int len = sizeof(bound);
        getsockname(s, reinterpret_cast<struct sockaddr*>(&bound), &len);
        port = ntohs(bound.sin_port);
    }
    listener_ = from_socket(s);
    return port;
}

std::optional<TcpChannel> TcpListener::accept() {
    if (listener_ == 0) return std::nullopt;
    SOCKET c = ::accept(to_socket(listener_), nullptr, nullptr);
    if (c == INVALID_SOCKET) return std::nullopt;
    return TcpChannel(from_socket(c));
}

}  // namespace fabric