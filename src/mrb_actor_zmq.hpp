#pragma once
#define ZMQ_BUILD_DRAFT_API
#include <mutex>
#include <optional>
#include <string>
#include <zmq.h>
#include <mruby.h>
#include <mruby/error.h>
#include <mruby/branch_pred.h>

static void *mrb_actor_zmq_context = nullptr;
static std::once_flag zmq_context_once;

struct ZmqSocket
{
  mrb_state *mrb;

  explicit ZmqSocket(mrb_state *mrb, int type) : mrb(mrb)
  {
    socket_ = zmq_socket(mrb_actor_zmq_context, type);
    if (unlikely(!socket_)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to create ZMQ socket");
    }
  }

  ~ZmqSocket()
  {
    if (likely(socket_)) {
      zmq_close(socket_);
      socket_ = nullptr;
    }
  }

  void
  bind(const char* endpoint)
  {
    if (unlikely(zmq_bind(socket_, endpoint) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to bind ZMQ socket");
    }
  }

  void
  connect(const char* endpoint)
  {
    if (unlikely(zmq_connect(socket_, endpoint) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to connect ZMQ socket");
    }
  }

  void*
  raw() const noexcept
  {
    return socket_;
  }

private:
  void* socket_ = nullptr;
};

struct ZmqMessage
{
  ZmqMessage(mrb_state *mrb) : mrb(mrb)
  {
    if (unlikely(zmq_msg_init(&_msg) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to initialize ZMQ message");
    }
  }

  explicit ZmqMessage(mrb_state *mrb, const void *string, size_t size) : mrb(mrb)
  {
    if (unlikely(zmq_msg_init_size(&_msg, size) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to initialize ZMQ message with size");
    }
    std::memcpy(zmq_msg_data(&_msg), string, size);
  }

  ~ZmqMessage()
  {
    zmq_msg_close(&_msg);
  }

  size_t size() const
  {
    return zmq_msg_size(const_cast<zmq_msg_t*>(&_msg));
  }

  const void* data() const
  {
    return zmq_msg_data(const_cast<zmq_msg_t*>(&_msg));
  }

  void* data()
  {
    return zmq_msg_data(&_msg);
  }

  bool more() const
  {
    return zmq_msg_more(const_cast<zmq_msg_t*>(&_msg)) != 0;
  }

  int
  recv(ZmqSocket &socket, int flags = 0)
  {
      int rc = zmq_msg_recv(&_msg, socket.raw(), flags);

      if (rc == -1) {
          errno = zmq_errno();

          // Non-blocking receive: no message available
          if ((flags & ZMQ_DONTWAIT) && errno == EAGAIN) {
              return -1; // caller interprets as "no message"
          }

          // Real error
          mrb_sys_fail(mrb, "Failed to receive ZMQ message");
      }

      return rc;
  }

  int
  send(ZmqSocket &socket, int flags = 0)
  {
    int rc = zmq_msg_send(&_msg, socket.raw(), flags);
    if (unlikely(rc == -1)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to send ZMQ message");
    }
    return rc;
  }

  private:
  mrb_state *mrb;
  zmq_msg_t _msg;
};