#pragma once
#include <utility>
#include <variant>
#include <stdexcept>
#include "fabric_scheduler/core/error.hpp"

namespace fabric {

// A lightweight Result for expected (non-fatal) outcomes such as revalidation.
template<typename T>
class Result {
public:
    static Result ok(T value) { return Result(std::in_place_index<0>, std::move(value)); }
    static Result err(ErrorCode code, std::string message) {
        return Result(std::in_place_index<1>, code, std::move(message));
    }

    Result(Result&&) noexcept = default;
    Result& operator=(Result&&) noexcept = default;
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;

    bool has_value() const noexcept { return storage_.index() == 0; }
    bool is_err() const noexcept { return storage_.index() == 1; }

    T& value() & {
        if (storage_.index() != 0) throw FabricError(ErrorCode::Internal, "Result has no value");
        return std::get<0>(storage_);
    }
    const T& value() const & {
        if (storage_.index() != 0) throw FabricError(ErrorCode::Internal, "Result has no value");
        return std::get<0>(storage_);
    }
    T&& value() && {
        if (storage_.index() != 0) throw FabricError(ErrorCode::Internal, "Result has no value");
        return std::move(std::get<0>(storage_));
    }

    ErrorCode error_code() const noexcept { return std::get<1>(storage_).code; }
    const std::string& error_message() const noexcept { return std::get<1>(storage_).message; }

    std::pair<ErrorCode, std::string> error() const { return { std::get<1>(storage_).code, std::get<1>(storage_).message }; }

private:
    struct Err { ErrorCode code; std::string message; };
    using Storage = std::variant<T, Err>;

    Result(std::in_place_index_t<0>, T v) : storage_(std::in_place_index<0>, std::move(v)) {}
    Result(std::in_place_index_t<1>, ErrorCode code, std::string msg)
        : storage_(std::in_place_index<1>, Err{ code, std::move(msg) }) {}

    Storage storage_;
};

template<>
class Result<void> {
public:
    static Result ok() { return Result(); }
    static Result err(ErrorCode code, std::string message) { return Result(code, std::move(message)); }

    bool has_value() const noexcept { return !has_error_; }
    bool is_err() const noexcept { return has_error_; }
    ErrorCode error_code() const noexcept { return code_; }
    const std::string& error_message() const noexcept { return message_; }
    std::pair<ErrorCode, std::string> error() const { return { code_, message_ }; }

private:
    Result() : has_error_(false), code_(ErrorCode::Internal) {}
    Result(ErrorCode code, std::string msg) : has_error_(true), code_(code), message_(std::move(msg)) {}
    bool has_error_;
    ErrorCode code_;
    std::string message_;
};

}  // namespace fabric
