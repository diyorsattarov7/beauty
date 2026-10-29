#pragma once

#include <beauty/version.hpp>
#include <beauty/utils.hpp>
#include <beauty/route.hpp>
#include <beauty/websocket_context.hpp>

#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace asio = boost::asio;
namespace beast = boost::beast;

namespace beauty {

// --------------------------------------------------------------------------
// Handles a WebSocket server connection
// --------------------------------------------------------------------------
class websocket_session : public std::enable_shared_from_this<websocket_session> {
public:
    explicit websocket_session(asio::ip::tcp::socket&& socket,
                               const beauty::route& route) :
        _strand(asio::make_strand(
                static_cast<asio::io_context&>(socket.get_executor().context()))),
        _websocket(std::move(socket)),
        _route(route),
        _admission(std::make_shared<admission_state>())
    {}

    ~websocket_session()
    {
        if (!_ws_context.uuid.empty())
            _route.disconnect(_ws_context);
    }

    void run(const beauty::request& req)
    {
        asio::post(_strand, [me = shared_from_this(), req] {
            me->do_run(req);
        });
    }

    void send(std::string&& msg, bool binary_mode = false)
    {
        const auto size = msg.size();
        bool overflow = false;

        {
            std::lock_guard<std::mutex> lock(_admission->mutex);
            if (_admission->stopping)
                return;

            if (_admission->messages >= max_queue_messages ||
                size > max_queue_bytes - _admission->bytes) {
                _admission->stopping = true;
                overflow = true;
            } else {
                ++_admission->messages;
                _admission->bytes += size;
            }
        }

        if (overflow) {
            asio::post(_strand, [me = shared_from_this()] {
                me->stop_connection();
            });
            return;
        }

        message outgoing{std::move(msg), binary_mode,
                         reservation(_admission, size)};
        asio::post(_strand,
                   [me = shared_from_this(), outgoing = std::move(outgoing)]() mutable {
                       me->enqueue(std::move(outgoing));
                   });
    }

private:
    static constexpr std::size_t max_queue_messages = 256;
    static constexpr std::size_t max_queue_bytes = 1024 * 1024;

    struct admission_state {
        std::mutex mutex;
        std::size_t messages{0};
        std::size_t bytes{0};
        bool stopping{false};
    };

    class reservation {
    public:
        reservation(std::shared_ptr<admission_state> state, std::size_t size) :
            _state(std::move(state)), _size(size)
        {}

        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;

        reservation(reservation&& other) noexcept :
            _state(std::move(other._state)), _size(other._size)
        {
            other._size = 0;
        }

        reservation& operator=(reservation&& other) noexcept
        {
            if (this != &other) {
                release();
                _state = std::move(other._state);
                _size = other._size;
                other._size = 0;
            }
            return *this;
        }

        ~reservation() { release(); }

    private:
        void release()
        {
            if (!_state)
                return;

            std::lock_guard<std::mutex> lock(_state->mutex);
            --_state->messages;
            _state->bytes -= _size;
            _state.reset();
        }

        std::shared_ptr<admission_state> _state;
        std::size_t _size{0};
    };

    struct message {
        std::string buffer;
        bool binary_mode;
        reservation capacity;
    };

    asio::strand<asio::io_context::executor_type> _strand;
    beast::websocket::stream<beast::tcp_stream> _websocket;
    beast::flat_buffer _buffer;
    const beauty::route& _route;
    ws_context _ws_context;
    std::shared_ptr<admission_state> _admission;
    std::deque<message> _queue;
    bool _stopped{false};
    bool _writing{false};

    bool is_stopping() const
    {
        std::lock_guard<std::mutex> lock(_admission->mutex);
        return _admission->stopping;
    }

    void mark_stopping()
    {
        std::lock_guard<std::mutex> lock(_admission->mutex);
        _admission->stopping = true;
    }

    void stop_connection()
    {
        if (_stopped)
            return;

        _stopped = true;
        mark_stopping();

        // Preserve the active write buffer until its completion handler.
        if (_writing) {
            while (_queue.size() > 1)
                _queue.pop_back();
        } else {
            _queue.clear();
        }

        beast::error_code ec;
        beast::get_lowest_layer(_websocket).socket().cancel(ec);
        beast::get_lowest_layer(_websocket).socket().close(ec);
    }

    void do_run(const beauty::request& req)
    {
        if (is_stopping()) {
            stop_connection();
            return;
        }

        beast::error_code ec;
        _ws_context.remote_endpoint =
            _websocket.next_layer().socket().remote_endpoint(ec);
        if (ec) {
            fail(ec, "remote_endpoint");
            stop_connection();
            return;
        }
        _ws_context.local_endpoint =
            _websocket.next_layer().socket().local_endpoint(ec);
        if (ec) {
            fail(ec, "local_endpoint");
            stop_connection();
            return;
        }
        _ws_context.target = std::string{req.target()};
        _ws_context.route_path = _route.path();
        _ws_context.attributes = req.get_attributes();

        _websocket.set_option(
            beast::websocket::stream_base::timeout::suggested(
                beast::role_type::server));
        _websocket.set_option(beast::websocket::stream_base::decorator(
            [](beast::websocket::response_type& res) {
                res.set(beast::http::field::server,
                        std::string(BEAUTY_PROJECT_VERSION) +
                        " beauty websocket");
            }));
        _websocket.read_message_max(16 * 1024);
        _websocket.async_accept(
            req,
            asio::bind_executor(_strand,
                [me = shared_from_this()](auto error) {
                    me->on_accept(error);
                }));
    }

    void on_accept(beast::error_code ec)
    {
        if (ec) {
            fail(ec, "accept");
            stop_connection();
            return;
        }
        if (is_stopping()) {
            stop_connection();
            return;
        }

        _ws_context.ws_session = shared_from_this();
        _ws_context.uuid = make_uuid();
        _route.connect(_ws_context);
        if (!is_stopping())
            do_read();
        else
            stop_connection();
    }

    void do_read()
    {
        _websocket.async_read(
            _buffer,
            asio::bind_executor(_strand,
                [me = shared_from_this()](auto ec, std::size_t size) {
                    me->on_read(ec, size);
                }));
    }

    void on_read(beast::error_code ec, std::size_t)
    {
        if (ec == beast::websocket::error::closed) {
            stop_connection();
            return;
        }
        if (ec) {
            fail(ec, "read");
            stop_connection();
            return;
        }
        if (is_stopping()) {
            stop_connection();
            return;
        }

        _route.receive(_ws_context,
                       static_cast<const char*>(_buffer.cdata().data()),
                       _buffer.size(), _websocket.got_text());
        _buffer.consume(_buffer.size());

        if (is_stopping())
            stop_connection();
        else
            do_read();
    }

    void enqueue(message&& msg)
    {
        if (is_stopping()) {
            stop_connection();
            return;
        }

        _queue.push_back(std::move(msg));
        if (!_writing)
            start_write();
    }

    void start_write()
    {
        _writing = true;
        _websocket.binary(_queue.front().binary_mode);
        _websocket.async_write(
            beast::net::buffer(_queue.front().buffer),
            asio::bind_executor(_strand,
                [me = shared_from_this()](auto ec, std::size_t size) {
                    me->on_write(ec, size);
                }));
    }

    void on_write(beast::error_code ec, std::size_t)
    {
        // The async_write has finished, so its buffer may now be destroyed.
        _writing = false;
        _queue.pop_front();

        if (ec) {
            fail(ec, "write");
            stop_connection();
            return;
        }
        if (is_stopping()) {
            stop_connection();
            return;
        }
        if (!_queue.empty())
            start_write();
    }
};

}

