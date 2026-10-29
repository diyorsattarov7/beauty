#pragma once

#include <beauty/header.hpp>

#include <boost/beast/http.hpp>

#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace beast = boost::beast;

namespace beauty
{
namespace http = beast::http;

// --------------------------------------------------------------------------
class response : public beast::http::response<beast::http::string_body>
{
public:
    using beast::http::response<beast::http::string_body>::response;
    using beast::http::response<beast::http::string_body>::set;
    using beast::http::response<beast::http::string_body>::operator=;

    void set(const beauty::header::content_type& ct) {
        set(beast::http::field::content_type, ct.value);
    }

    void set_header(beast::http::field name, const std::string& value) {
        set(name, value);
    }

    bool is_postponed() const { return _is_postponed; }
    void postpone() { _is_postponed = true; }

    void done()
    {
        std::function<void()> cb;

        {
            std::lock_guard<std::mutex> lock(_completion->mutex);

            if (_completion->done)
                return;

            _completion->done = true;
            cb = std::move(_completion->cb);
        }

        if (cb)
            cb();
    }

    void on_done(std::function<void()>&& cb)
    {
        bool invoke = false;

        {
            std::lock_guard<std::mutex> lock(_completion->mutex);

            if (_completion->done) {
                invoke = true;
            } else {
                _completion->cb = std::move(cb);
            }
        }

        if (invoke && cb)
            cb();
    }
    // Result alias and a common helper for no error
    http::status status() const { return result(); }
    bool is_status_ok() const { return status() == http::status::ok; }

private:
    bool    _is_postponed = false;
    struct completion_state {
        std::mutex mutex;
        bool done{false};
        std::function<void()> cb;
    };

    std::shared_ptr<completion_state> _completion =
        std::make_shared<completion_state>();
};
}
