#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mruby.h>
#include <mruby/presym.h>
#include <stdexcept>
#include <thread>
#define ZMQ_BUILD_DRAFT_API
#include <zmq.h>
#include <mutex>
#include <mruby/cpp_helpers.hpp>
#include <mruby/class.h>
#include <mruby/data.h>
#include <sstream>
#define MSGPACK_NO_BOOST
#define MSGPACK_DEFAULT_API_VERSION 3
#include <msgpack.hpp>
#include <mruby/msgpack.h>
#include <exception>
#include <mruby/string.h>
#include <mruby/branch_pred.h>
#include <mruby/error.h>
#include <mruby/array.h>
MRB_BEGIN_DECL
#include <mruby/internal.h>
MRB_END_DECL
#include <mruby/hash.h>
#include <mruby/variable.h>
#include <mruby/num_helpers.hpp>

static void *mrb_actor_zmq_context = nullptr;
static std::once_flag zmq_context_once;

struct ZmqSocket
{
  mrb_state *mrb = nullptr;

  explicit ZmqSocket(mrb_state *mrb, int type) : mrb(mrb)
  {
    socket_ = zmq_socket(mrb_actor_zmq_context, type);
    if (unlikely(!socket_)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to create ZMQ socket");
    }
  }

  // Non-copyable
  ZmqSocket(const ZmqSocket&) = delete;
  ZmqSocket& operator=(const ZmqSocket&) = delete;

  // Non-movable
  ZmqSocket(ZmqSocket&&) = delete;
  ZmqSocket& operator=(ZmqSocket&&) = delete;

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
    if (unlikely(zmq_msg_init(&msg) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to initialize ZMQ message");
    }
  }

  explicit ZmqMessage(mrb_state *mrb, const void *string, size_t size) : mrb(mrb)
  {
    if (unlikely(zmq_msg_init_size(&msg, size) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to initialize ZMQ message with size");
    }
    std::memcpy(zmq_msg_data(&msg), string, size);
  }

  ~ZmqMessage()
  {
    zmq_msg_close(&msg);
  }

  ZmqMessage(const ZmqMessage&) = delete;
  ZmqMessage& operator=(const ZmqMessage&) = delete;
  ZmqMessage(ZmqMessage&&) = delete;
  ZmqMessage& operator=(ZmqMessage&&) = delete;

  size_t size() const
  {
    return zmq_msg_size(const_cast<zmq_msg_t*>(&msg));
  }

  const void* data() const
  {
    return zmq_msg_data(const_cast<zmq_msg_t*>(&msg));
  }

  void* data()
  {
    return zmq_msg_data(&msg);
  }

  bool more() const
  {
    return zmq_msg_more(const_cast<zmq_msg_t*>(&msg)) != 0;
  }

  int
  recv(ZmqSocket &socket, int flags = 0)
  {
    int rc = zmq_msg_recv(&msg, socket.raw(), flags);
    if (unlikely(rc == -1)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to receive ZMQ message");
    }
    return rc;
  }

  int
  send(ZmqSocket &socket, int flags = 0)
  {
    int rc = zmq_msg_send(&msg, socket.raw(), flags);
    if (unlikely(rc == -1)) {
      errno = zmq_errno();
      mrb_sys_fail(mrb, "Failed to send ZMQ message");
    }
    return rc;
  }

  private:
  mrb_state *mrb = nullptr;
  zmq_msg_t msg;

  static void
  ensure_pair_socket(ZmqSocket &socket)
  {
    int type = 0;
    size_t size = sizeof(type);

    if (unlikely(zmq_getsockopt(socket.raw(), ZMQ_TYPE, &type, &size) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(socket.mrb, "Failed to query ZMQ socket type");
    }

    if (unlikely(type != ZMQ_PAIR)) {
      mrb_state *mrb = socket.mrb;
      mrb_raise(mrb, E_ARGUMENT_ERROR,
        "send_struct() may only be used on ZMQ_PAIR sockets"
      );
    }
  }

  static void
  ensure_inproc_socket(ZmqSocket &socket)
  {
    char endpoint[256];
    size_t size = sizeof(endpoint);

    if (unlikely(zmq_getsockopt(socket.raw(), ZMQ_LAST_ENDPOINT, endpoint, &size) != 0)) {
      errno = zmq_errno();
      mrb_sys_fail(
        socket.mrb,
        "Failed to query ZMQ socket endpoint"
      );
    }

    // Must start with "inproc://"
    if (unlikely(std::strncmp(endpoint, "inproc://", 9) != 0)) {
      mrb_state *mrb = socket.mrb;
      mrb_raise(mrb, E_ARGUMENT_ERROR, "send_struct() may only be used on inproc:// PAIR sockets");
    }
  }
};


struct Opcode
{
  enum class Code : uint8_t
  {
    CREATE_OBJECT_INPROC          = 1,
    CALL_METHOD_INPROC            = 2,
    CALL_METHOD_INPROC_AND_FORGET = 3,
    DESTROY_OBJECT_INPROC         = 4,
    SHUTDOWN                      = 5,

    RETURN_VALUE_INPROC           = 10,
    RAISE_EXCEPTION               = 11,
    OBJECT_CREATED_INPROC         = 12,
    READY                         = 13,

    MALFORMED_OPCODE              = 255
  };

  static int
  send(ZmqSocket &socket, Code op, int flags = 0)
  {
    ZmqMessage m(socket.mrb, &op, sizeof(op));
    return m.send(socket, flags);
  }

  static std::pair<uint8_t, bool>
  recv(ZmqSocket &socket)
  {
    ZmqMessage msg(socket.mrb);
    msg.recv(socket);

    if (unlikely(msg.size() != sizeof(Code))) {
      return {static_cast<uint8_t>(Code::MALFORMED_OPCODE), msg.more()} ;
    }

    const uint8_t byte =
      *static_cast<const uint8_t*>(msg.data());

    return {byte, msg.more()};
  }
};

class ActorThreadContext
{
  std::string endpoint;
  mrb_state *mrb = nullptr;
  std::optional<ZmqSocket> pair_socket;
  std::optional<ZmqSocket> router_socket;

  public:

  const mrb_state *get_mrb() const noexcept
  {
    return mrb;
  }

  ActorThreadContext(const std::string& addr) : endpoint(addr)
  {
    mrb = mrb_open();
    if (unlikely(!mrb)) {
      throw std::runtime_error("Failed to create mruby state in actor thread");
    }
    pair_socket.emplace(mrb, ZMQ_PAIR);
    pair_socket->connect(endpoint.c_str());
    router_socket.emplace(mrb, ZMQ_ROUTER);
    mrb_gv_set(mrb, MRB_SYM(__actor_objects__), mrb_hash_new(mrb));

    Opcode::send(*pair_socket, Opcode::Code::READY);
  }

  ~ActorThreadContext()
  {
    if (likely(mrb)) {
      mrb_close(mrb);
      mrb = nullptr;
    }
  }

  void handle_opcode_create_object_inproc()
  {
    ZmqMessage class_id_msg(mrb);
    int rc = class_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_sym))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed class ID in CREATE_OBJECT_INPROC");
    }
    const mrb_sym class_id =
      *static_cast<const mrb_sym*>(class_id_msg.data());

    ZmqMessage args_msg(mrb);
    rc = args_msg.recv(*pair_socket);
    mrb_value args_str = mrb_str_new_static(mrb,
      static_cast<const char*>(args_msg.data()),
      args_msg.size()
    );
    mrb_value args_value = mrb_msgpack_unpack(mrb, args_str);
    if (unlikely(!mrb_array_p(args_value) && !mrb_nil_p(args_value))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Args argument is not an Array or nil");
    }

    ZmqMessage blk_msg(mrb);
    blk_msg.recv(*pair_socket);
    mrb_value blk_str = mrb_str_new_static(mrb,
      static_cast<const char*>(blk_msg.data()),
      blk_msg.size()
    );
    mrb_value blk_value = mrb_msgpack_unpack(mrb, blk_str);
    if (unlikely(!mrb_nil_p(blk_value) && !mrb_proc_p(blk_value))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Block argument is not a Proc or nil");
    }

    mrb_value klass = mrb_obj_value(mrb_class_get_id(mrb, class_id));
    mrb_value obj = mrb_instance_new(mrb, klass);
    obj = mrb_funcall_with_block(mrb, obj, MRB_SYM(initialize), mrb_array_p(args_value) ? RARRAY_LEN(args_value) : 0,
        mrb_array_p(args_value) ? RARRAY_PTR(args_value) : nullptr,
        blk_value
      );

    if (mrb->exc) {
      mrb_value exc = mrb_obj_value(mrb->exc);
      mrb_clear_error(mrb);
      mrb_value packed = mrb_msgpack_pack(mrb, exc);

      Opcode::send(*pair_socket, Opcode::Code::RAISE_EXCEPTION, ZMQ_SNDMORE);
      ZmqMessage exc_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      exc_msg.send(*pair_socket);
    } else {
      mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__actor_objects__));
      assert(mrb_hash_p(actor_objects));
      mrb_int object_id = mrb_obj_id(obj);
      mrb_hash_set(mrb, actor_objects, mrb_convert_number(mrb, object_id), obj);
      Opcode::send(*pair_socket, Opcode::Code::OBJECT_CREATED_INPROC, ZMQ_SNDMORE);
      ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
      obj_id_msg.send(*pair_socket);
    }
  }

  mrb_value
  handle_opcode_call_method_inproc_and_forget()
  {
    ZmqMessage obj_id_msg(mrb);
    int rc = obj_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed object ID in CALL_METHOD_INPROC_AND_FORGET");
    }
    const mrb_int object_id =
      *static_cast<const mrb_int*>(obj_id_msg.data());
    mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__actor_objects__));
    assert(mrb_hash_p(actor_objects));
    mrb_value obj_id_key = mrb_int_value(mrb, object_id);
    mrb_value target_obj = mrb_hash_get(mrb, actor_objects, obj_id_key);
    if (unlikely(mrb_nil_p(target_obj))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Object ID not found in CALL_METHOD_INPROC_AND_FORGET");
    }

    ZmqMessage func_msg(mrb);
    rc = func_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_sym))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed method ID in CALL_METHOD_INPROC_AND_FORGET");
    }
    const mrb_sym method_id =
      *static_cast<const mrb_sym*>(func_msg.data());

    ZmqMessage arg_msg(mrb);
    rc = arg_msg.recv(*pair_socket);
    mrb_value arg_str = mrb_str_new_static(mrb,
      static_cast<const char*>(arg_msg.data()),
      arg_msg.size()
    );
    mrb_value arg_value = mrb_msgpack_unpack(mrb, arg_str);
    if (unlikely(!mrb_array_p(arg_value) && !mrb_nil_p(arg_value))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Args argument is not an Array or nil");
    }

    ZmqMessage blk_msg(mrb);
    blk_msg.recv(*pair_socket);
    mrb_value blk_str = mrb_str_new_static(mrb,
      static_cast<const char*>(blk_msg.data()),
      blk_msg.size()
    );
    mrb_value blk_value = mrb_msgpack_unpack(mrb, blk_str);
    if (unlikely(!mrb_nil_p(blk_value) && !mrb_proc_p(blk_value))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Block argument is not a Proc or nil");
    }

    return mrb_funcall_with_block(mrb, target_obj, method_id,
        mrb_array_p(arg_value) ? RARRAY_LEN(arg_value) : 0,
        mrb_array_p(arg_value) ? RARRAY_PTR(arg_value) : nullptr,
        blk_value
      );
  }

  void handle_opcode_call_method_inproc()
  {
    mrb_value ret = handle_opcode_call_method_inproc_and_forget();
    if (mrb->exc) {
      mrb_value exc = mrb_obj_value(mrb->exc);
      mrb_clear_error(mrb);
      mrb_value packed = mrb_msgpack_pack(mrb, exc);
      Opcode::send(*pair_socket, Opcode::Code::RAISE_EXCEPTION, ZMQ_SNDMORE);
      ZmqMessage exc_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      exc_msg.send(*pair_socket);
    } else {
      mrb_value packed = mrb_msgpack_pack(mrb, ret);
      Opcode::send(*pair_socket, Opcode::Code::RETURN_VALUE_INPROC, ZMQ_SNDMORE);
      ZmqMessage ret_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      ret_msg.send(*pair_socket);
    }
  }

  void handle_opcode_destroy_object_inproc()
  {
    ZmqMessage obj_id_msg(mrb);
    int rc = obj_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed object ID in DESTROY_OBJECT_INPROC");
    }
    const mrb_int object_id =
      *static_cast<const mrb_int*>(obj_id_msg.data());
    mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__actor_objects__));
    assert(mrb_hash_p(actor_objects));
    mrb_value obj_id_key = mrb_int_value(mrb, object_id);
    mrb_hash_delete_key(mrb, actor_objects, obj_id_key);
  }

  void
  run_actor()
  {
    zmq_pollitem_t items[] = {
      { pair_socket->raw(),   0, ZMQ_POLLIN, 0 },
      { router_socket->raw(), 0, ZMQ_POLLIN, 0 }
    };

    while (true) {
      int rc = zmq_poll(items, 2, -1);
      if (rc <= 0) {
        break;
      }

      if (items[0].revents & ZMQ_POLLIN) {
        std::pair <uint8_t, bool> frame = Opcode::recv(*pair_socket);



        // TODO: dispatch opcode here

        if (frame.first == static_cast<uint8_t>(Opcode::Code::SHUTDOWN)) {
          break;
        } else if (frame.first == static_cast<uint8_t>(Opcode::Code::CREATE_OBJECT_INPROC)) {
          handle_opcode_create_object_inproc();
        } else if (frame.first == static_cast<uint8_t>(Opcode::Code::CALL_METHOD_INPROC)) {
          handle_opcode_call_method_inproc();
        } else if (frame.first == static_cast<uint8_t>(Opcode::Code::CALL_METHOD_INPROC_AND_FORGET)) {
          handle_opcode_call_method_inproc_and_forget();
        } else if (frame.first == static_cast<uint8_t>(Opcode::Code::DESTROY_OBJECT_INPROC)) {
          handle_opcode_destroy_object_inproc();
        }
      }
      if (items[1].revents & ZMQ_POLLIN) {
        // TODO: external routing
      }

      items[0].revents = 0;
      items[1].revents = 0;
      mrb_gc_arena_restore(mrb, 0);
    }
  }
};

class mrb_actor_mailbox
{
  mrb_state *owner_mrb = nullptr;
  std::thread thread;
  std::string endpoint;
  ZmqSocket owner_pair_socket;

  std::optional<ActorThreadContext> ctx;

  std::string
  make_mailbox_endpoint()
  {
    std::ostringstream oss;
    oss << "inproc://mailbox-" << this;
    return oss.str();
  }

  public:

  mrb_actor_mailbox(mrb_state *owner_mrb) : owner_mrb(owner_mrb), owner_pair_socket(owner_mrb, ZMQ_PAIR)
  {
    mrb_state *mrb = owner_mrb;
    endpoint = make_mailbox_endpoint();

    owner_pair_socket.bind(endpoint.c_str());

    // Create actor thread context
    ctx.emplace(endpoint);

    std::pair <uint8_t, bool> frame = Opcode::recv(owner_pair_socket);
    if (frame.first != static_cast<uint8_t>(Opcode::Code::READY)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Failed to receive READY from actor thread");
    }
  }

  ~mrb_actor_mailbox()
  {
    if (thread.joinable()) {
      Opcode::send(owner_pair_socket, Opcode::Code::SHUTDOWN);
      thread.join();
    }
  }

  mrb_actor_mailbox(const mrb_actor_mailbox&) = delete;
  mrb_actor_mailbox& operator=(const mrb_actor_mailbox&) = delete;

  mrb_actor_mailbox(mrb_actor_mailbox&&) = delete;
  mrb_actor_mailbox& operator=(mrb_actor_mailbox&&) = delete;

  void run()
  {
    thread = std::thread([this] {
      ctx->run_actor();
    });
  }

  mrb_value
  create_inproc_object(struct RClass *klass,
                         mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    mrb_class_path(mrb, klass);
    mrb_sym nsym = MRB_SYM(__classname__);
    mrb_value path = mrb_obj_iv_get(mrb, (struct RObject*)klass, nsym);
    if(!mrb_symbol_p(path)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Failed to get class path");
    }
    Opcode::send(owner_pair_socket, Opcode::Code::CREATE_OBJECT_INPROC, ZMQ_SNDMORE);

    mrb_sym class_id = mrb_symbol(path);
    ZmqMessage class_id_msg(mrb, &class_id, sizeof(class_id));
    class_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    // Send args
    mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
    ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
    args_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    // Send blk
    mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
    ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
    blk_msg.send(owner_pair_socket);

    // Wait for OBJECT_CREATED response
    std::pair <uint8_t, bool> frame = Opcode::recv(owner_pair_socket);
    if (frame.first != static_cast<uint8_t>(Opcode::Code::OBJECT_CREATED_INPROC)) {
      if (frame.first == static_cast<uint8_t>(Opcode::Code::RAISE_EXCEPTION)) {
        ZmqMessage exc_msg(mrb);
        exc_msg.recv(owner_pair_socket);
        mrb_value exc_str = mrb_str_new_static(mrb,
          static_cast<const char*>(exc_msg.data()),
          exc_msg.size()
        );
        mrb_value exc = mrb_msgpack_unpack(mrb, exc_str);
        mrb_exc_raise(mrb, exc);
      }
    }

    ZmqMessage obj_id_msg(mrb);
    obj_id_msg.recv(owner_pair_socket);
    if (obj_id_msg.size() != sizeof(mrb_int)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed OBJECT_CREATED object ID");
    }
    return mrb_int_value(mrb, *static_cast<const mrb_int*>(obj_id_msg.data()));
  }

  mrb_value
  call_method_inproc(mrb_int object_id, mrb_sym method_id,
                     mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::Code::CALL_METHOD_INPROC, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    ZmqMessage method_id_msg(mrb, &method_id, sizeof(method_id));
    method_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
    ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
    args_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
    ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
    blk_msg.send(owner_pair_socket);

    std::pair <uint8_t, bool> frame = Opcode::recv(owner_pair_socket);
    if (frame.first == static_cast<uint8_t>(Opcode::Code::RETURN_VALUE_INPROC)) {
      ZmqMessage ret_msg(mrb);
      ret_msg.recv(owner_pair_socket);
      mrb_value ret_str = mrb_str_new_static(mrb,
        static_cast<const char*>(ret_msg.data()),
        ret_msg.size()
      );
      return mrb_msgpack_unpack(mrb, ret_str);
    } else if (frame.first == static_cast<uint8_t>(Opcode::Code::RAISE_EXCEPTION)) {
      ZmqMessage exc_msg(mrb);
      exc_msg.recv(owner_pair_socket);
      mrb_value exc_str = mrb_str_new_static(mrb,
        static_cast<const char*>(exc_msg.data()),
        exc_msg.size()
      );
      mrb_value exc = mrb_msgpack_unpack(mrb, exc_str);
      if (unlikely(!mrb_exception_p(exc))) {
        mrb_raise(mrb, E_RUNTIME_ERROR, "Received non-exception object in RAISE_EXCEPTION");
      }
      mrb_exc_raise(mrb, exc);
    } else {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed response opcode in CALL_METHOD_INPROC");
    }
    return mrb_undef_value(); // Unreachable
  }

  void
  call_method_inproc_and_forget(mrb_int object_id, mrb_sym method_id,
                     mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::Code::CALL_METHOD_INPROC_AND_FORGET, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    ZmqMessage method_id_msg(mrb, &method_id, sizeof(method_id));
    method_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
    ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
    args_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
    ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
    blk_msg.send(owner_pair_socket);
  }

  void
  destroy_inproc_object(mrb_int object_id)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::Code::DESTROY_OBJECT_INPROC);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket);
  }
};

MRB_CPP_DEFINE_TYPE(mrb_actor_mailbox, mrb_actor_mailbox);

static mrb_value
mrb_actor_new(mrb_state* mrb, mrb_value self)
{
  mrb_actor_mailbox* mailbox = mrb_cpp_new<mrb_actor_mailbox>(mrb, self, mrb);
  mailbox->run();

  return self;
}

struct mrb_actor_object {
  mrb_actor_mailbox* mailbox;
  mrb_int object_id;

  mrb_actor_object(mrb_actor_mailbox* mailbox, mrb_int object_id)
    : mailbox(mailbox), object_id(object_id) {}

  ~mrb_actor_object()
  {
    mailbox->destroy_inproc_object(object_id);
  }
};

MRB_CPP_DEFINE_TYPE(mrb_actor_object, mrb_actor_object);

static mrb_value
mrb_actor_object_initialize(mrb_state* mrb, mrb_value self)
{
  mrb_actor_mailbox* mailbox = nullptr;
  mrb_int object_id = 0;
  mrb_get_args(mrb, "di", &mailbox, &mrb_actor_mailbox_type, &object_id);
  mrb_cpp_new<mrb_actor_object>(mrb, self, mailbox, object_id);

  return self;
}

static mrb_value
mrb_actor_object_call_method(mrb_state* mrb, mrb_value self)
{
  mrb_actor_object* actor_obj = static_cast<mrb_actor_object*>(
    mrb_data_get_ptr(mrb, self, &mrb_actor_object_type)
  );

  mrb_sym method_id;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;
  mrb_value blk = mrb_nil_value();
  mrb_get_args(mrb, "n*&", &method_id, &argv, &argc, &blk);

  return actor_obj->mailbox->call_method_inproc(
    actor_obj->object_id, method_id, argv, argc, blk
  );
}

static mrb_value
mrb_actor_object_call_method_and_forget(mrb_state* mrb, mrb_value self)
{
  mrb_actor_object* actor_obj = static_cast<mrb_actor_object*>(
    mrb_data_get_ptr(mrb, self, &mrb_actor_object_type)
  );
  mrb_sym method_id;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;
  mrb_value blk = mrb_nil_value();
  mrb_get_args(mrb, "n*&", &method_id, &argv, &argc, &blk);
  actor_obj->mailbox->call_method_inproc_and_forget(
    actor_obj->object_id, method_id, argv, argc, blk
  );
  return mrb_nil_value();
}

static mrb_value
mrb_actor_create_inproc_object(mrb_state* mrb, mrb_value self)
{
  mrb_actor_mailbox* mailbox = static_cast<mrb_actor_mailbox*>(
    mrb_data_get_ptr(mrb, self, &mrb_actor_mailbox_type)
  );

  struct RClass* klass;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;
  mrb_value blk = mrb_nil_value();

  mrb_get_args(mrb, "c*&", &klass, &argv, &argc, &blk);

  mrb_value object_id = mailbox->create_inproc_object(klass, argv, argc, blk);
  return mrb_obj_new(mrb, mrb_class_get_under_id(mrb, mrb_class(mrb, self), MRB_SYM(ActorInprocObject)), 2,
    (mrb_value[]){ self, object_id }
  );
}

MRB_BEGIN_DECL
void
mrb_actor_global_shutdown()
{
  if (!mrb_actor_zmq_context) return;

  zmq_ctx_shutdown(mrb_actor_zmq_context);
  zmq_ctx_destroy(mrb_actor_zmq_context);
  mrb_actor_zmq_context = nullptr;
}

void
mrb_mruby_actor_gem_init(mrb_state* mrb)
{
  std::call_once(zmq_context_once, [&] {
    mrb_actor_zmq_context = zmq_ctx_new();
    std::atexit(mrb_actor_global_shutdown);
  });
  if (unlikely(!mrb_actor_zmq_context)) {
    mrb_sys_fail(mrb, "Failed to create ZMQ context");
  }

  struct RClass *actor_class, *actor_inproc_object_class;
  actor_class = mrb_define_class_id(mrb, MRB_SYM(Actor), mrb->object_class);
  MRB_SET_INSTANCE_TT(actor_class, MRB_TT_DATA);
  mrb_define_method_id(mrb, actor_class, MRB_SYM(initialize), mrb_actor_new, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, actor_class, MRB_SYM(new_inproc), mrb_actor_create_inproc_object,
    MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK()|MRB_ARGS_REST());
  actor_inproc_object_class = mrb_define_class_under_id(mrb, actor_class, MRB_SYM(ActorInprocObject), mrb->object_class);
  MRB_SET_INSTANCE_TT(actor_inproc_object_class, MRB_TT_DATA);
  mrb_define_method_id(mrb, actor_inproc_object_class, MRB_SYM(initialize), mrb_actor_object_initialize,
    MRB_ARGS_REQ(2));
  mrb_define_method_id(mrb, actor_inproc_object_class, MRB_SYM(call), mrb_actor_object_call_method,
    MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK()|MRB_ARGS_REST());
  mrb_define_method_id(mrb, actor_inproc_object_class, MRB_SYM(call_and_forget), mrb_actor_object_call_method_and_forget,
    MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK()|MRB_ARGS_REST());
}

void
mrb_mruby_actor_gem_final(mrb_state* mrb) {}
MRB_END_DECL
