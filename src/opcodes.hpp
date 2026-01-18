#pragma once
#include "mrb_actor_zmq.hpp"
#include <mruby/branch_pred.h>

struct Opcode
{
  enum : uint8_t
  {
    READY                  = 0,
    SHUTDOWN               = 1,

    CREATE_OBJECT          = 20,
    DESTROY_OBJECT         = 21,
    OBJECT_CREATED         = 22,

    CALL_METHOD            = 40,
    CALL_METHOD_AND_FORGET = 41,
    RETURN_VALUE           = 42,
    RAISE_EXCEPTION        = 43,

    CALL_FUTURE            = 60,
    FUTURE_VALUE           = 61,
    FUTURE_EXCEPTION       = 62,

    TIMEOUT                = 100,

    MALFORMED_OPCODE       = 255
  };

  static int
  send(ZmqSocket &socket, uint8_t op, int flags = 0)
  {
    ZmqMessage m(socket.mrb, &op, sizeof(op));
    return m.send(socket, flags);
  }

  static std::pair<uint8_t, bool>
  recv(ZmqSocket &socket, int flags = 0)
  {
      ZmqMessage msg(socket.mrb);
      int rc = msg.recv(socket, flags);

      if (rc == -1) {
          errno = zmq_errno();

          // Non-blocking: no message available
          if ((flags & ZMQ_DONTWAIT) && errno == EAGAIN) {
              return {static_cast<uint8_t>(TIMEOUT), false};
          }

          // Real error → propagate
          mrb_sys_fail(socket.mrb, "Failed to receive ZMQ message");
      }

      // Normal path
      if (unlikely(msg.size() != sizeof(uint8_t))) {
          return {static_cast<uint8_t>(MALFORMED_OPCODE), msg.more()};
      }

      const uint8_t byte =
          *static_cast<const uint8_t*>(msg.data());

      return {byte, msg.more()};
  }
};