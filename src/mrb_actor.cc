#define MSGPACK_NO_BOOST
#define MSGPACK_DEFAULT_API_VERSION 3
#include <msgpack.hpp>
#include "base_virtual_actor_context.hpp"
#include "base_virtual_actor_mailbox.hpp"
#include "mrb_actor_zmq.hpp"
#include "opcodes.hpp"
#include <mruby/branch_pred.h>
#include <mruby/msgpack.h>
#include <mruby/array.h>
#include <mruby/error.h>
#include <mruby/string.h>
#include <mruby/presym.h>
#include <mruby/variable.h>
#include <mruby/num_helpers.hpp>
#include <mruby/hash.h>
#include <mruby/proc.h>
#include <mruby/class.h>
#include <thread>
#include <mruby/zmq.h>
#include "util.hpp"

class InprocActorThreadContext
{
  protected:
  std::string endpoint;
  std::optional<ZmqSocket> pair_socket;
  mrb_value actor_classes = mrb_undef_value();
  mrb_state *mrb = nullptr;

  public:

  InprocActorThreadContext(const std::string& addr) : endpoint(addr)
  {
  }

  ~InprocActorThreadContext()
  {
    if (mrb) {
      mrb_close(mrb);
      mrb = nullptr;
    }
  }

  void handle_opcode_create_object()
  {
    ZmqMessage seq_msg(mrb);
    int rc = seq_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(uint64_t))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed sequence number in CREATE_OBJECT");
    }

    ZmqMessage class_id_msg(mrb);
    rc = class_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_sym))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed class ID in CREATE_OBJECT");
    }
    const mrb_sym class_id =
      *static_cast<const mrb_sym*>(class_id_msg.data());

    mrb_value args_value = mrb_nil_value();

    if (class_id_msg.more()) {
      ZmqMessage args_msg(mrb);
      args_msg.recv(*pair_socket);
      mrb_value args_str = mrb_str_new_static(mrb,
        static_cast<const char*>(args_msg.data()),
        args_msg.size()
      );
      args_value = mrb_msgpack_unpack(mrb, args_str);
    }

    mrb_value obj = mrb_obj_new(mrb, mrb_class_get_id(mrb, class_id),
        mrb_array_p(args_value) ? RARRAY_LEN(args_value) : 0,
        mrb_array_p(args_value) ? RARRAY_PTR(args_value) : nullptr
      );

    if (unlikely(mrb->exc)) {
      mrb_value exc = mrb_obj_value(mrb->exc);
      mrb_clear_error(mrb);
      mrb_value packed = mrb_msgpack_pack(mrb, exc);

      Opcode::send(*pair_socket, Opcode::RAISE_EXCEPTION, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage exc_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      exc_msg.send(*pair_socket);
    } else {
      mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__mrb_actor_objects__));
      mrb_assert(mrb_hash_p(actor_objects));
      mrb_int object_id = mrb_obj_id(obj);
      mrb_hash_set(mrb, actor_objects, mrb_convert_number(mrb, object_id), obj);
      Opcode::send(*pair_socket, Opcode::OBJECT_CREATED, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
      obj_id_msg.send(*pair_socket);
    }
  }

  mrb_value
  handle_opcode_call_method_and_forget()
  {
    ZmqMessage obj_id_msg(mrb);
    int rc = obj_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed object ID in CALL_METHOD_AND_FORGET");
    }
    const mrb_int object_id =
      *static_cast<const mrb_int*>(obj_id_msg.data());
    mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__mrb_actor_objects__));
    mrb_assert(mrb_hash_p(actor_objects));
    mrb_value obj_id_key = mrb_convert_number(mrb, object_id);
    mrb_value target_obj = mrb_hash_fetch(mrb, actor_objects, obj_id_key, mrb_undef_value());
    if (unlikely(mrb_undef_p(target_obj))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Object ID not found in CALL_METHOD_AND_FORGET");
    }

    ZmqMessage func_msg(mrb);
    rc = func_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_sym))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed method ID in CALL_METHOD_AND_FORGET");
    }
    const mrb_sym method_id =
      *static_cast<const mrb_sym*>(func_msg.data());

    mrb_value arg_value = mrb_nil_value();
    mrb_value blk_value = mrb_nil_value();

    // Check if args frame exists
    if (func_msg.more()) {
      ZmqMessage args_msg(mrb);
      args_msg.recv(*pair_socket);
      mrb_value args_str = mrb_str_new_static(mrb,
        static_cast<const char*>(args_msg.data()),
        args_msg.size()
      );
      arg_value = mrb_msgpack_unpack(mrb, args_str);
      if (args_msg.more()) {
        ZmqMessage blk_msg(mrb);
        blk_msg.recv(*pair_socket);

        mrb_value blk_str = mrb_str_new_static(mrb,
          static_cast<const char*>(blk_msg.data()),
          blk_msg.size()
        );
        blk_value = mrb_msgpack_unpack(mrb, blk_str);
        // After reading block frame (if present)
        if (blk_msg.more()) {
            // Protocol violation: unexpected extra frames
            // In inproc mode, safest action is to abort
            mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed message: unexpected extra frames");
        }
      }
    }

    return mrb_funcall_with_block(mrb, target_obj, method_id,
        mrb_array_p(arg_value) ? RARRAY_LEN(arg_value) : 0,
        mrb_array_p(arg_value) ? RARRAY_PTR(arg_value) : nullptr,
        blk_value
      );
  }

  void handle_opcode_call_future()
  {
    ZmqMessage seq_msg(mrb);
    int rc = seq_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(uint64_t))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed sequence number in CALL_METHOD_AND_FORGET");
    }

    mrb_value ret = handle_opcode_call_method_and_forget();
    if (unlikely(mrb->exc)) {
      mrb_value exc = mrb_obj_value(mrb->exc);
      mrb_clear_error(mrb);
      mrb_value packed = mrb_msgpack_pack(mrb, exc);
      Opcode::send(*pair_socket, Opcode::FUTURE_EXCEPTION, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage exc_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      exc_msg.send(*pair_socket);
    } else {
      mrb_value packed = mrb_msgpack_pack(mrb, ret);
      Opcode::send(*pair_socket, Opcode::FUTURE_VALUE, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage ret_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      ret_msg.send(*pair_socket);
    }
  }

  void handle_opcode_call_method()
  {
    ZmqMessage seq_msg(mrb);
    int rc = seq_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(uint64_t))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed sequence number in CALL_METHOD_AND_FORGET");
    }

    mrb_value ret = handle_opcode_call_method_and_forget();
    if (unlikely(mrb->exc)) {
      mrb_value exc = mrb_obj_value(mrb->exc);
      mrb_clear_error(mrb);
      mrb_value packed = mrb_msgpack_pack(mrb, exc);
      Opcode::send(*pair_socket, Opcode::RAISE_EXCEPTION, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage exc_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      exc_msg.send(*pair_socket);
    } else {
      mrb_value packed = mrb_msgpack_pack(mrb, ret);
      Opcode::send(*pair_socket, Opcode::RETURN_VALUE, ZMQ_SNDMORE);
      seq_msg.send(*pair_socket, ZMQ_SNDMORE);
      ZmqMessage ret_msg(mrb, RSTRING_PTR(packed), RSTRING_LEN(packed));
      ret_msg.send(*pair_socket);
    }
  }

  void handle_opcode_destroy_object()
  {
    ZmqMessage obj_id_msg(mrb);
    int rc = obj_id_msg.recv(*pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_ARGUMENT_ERROR, "Malformed object ID in DESTROY_OBJECT");
    }
    const mrb_int object_id =
      *static_cast<const mrb_int*>(obj_id_msg.data());
    mrb_value actor_objects = mrb_gv_get(mrb, MRB_SYM(__mrb_actor_objects__));
    mrb_assert(mrb_hash_p(actor_objects));
    mrb_value obj_id_key = mrb_convert_number(mrb, object_id);
    mrb_hash_delete_key(mrb, actor_objects, obj_id_key);
  }

  static mrb_value
  mrb_actor_step(mrb_state* mrb, mrb_value self)
  {
    mrb_value env = mrb_proc_cfunc_env_get(mrb, 0);
    InprocActorThreadContext *ctx = static_cast<InprocActorThreadContext*>(mrb_cptr(env));

    std::pair <uint8_t, bool> frame = Opcode::recv(*ctx->pair_socket);
    if (unlikely(frame.first == static_cast<uint8_t>(Opcode::SHUTDOWN))) {
      return mrb_false_value();
    } else if (frame.first == static_cast<uint8_t>(Opcode::CALL_METHOD_AND_FORGET)) {
      ctx->handle_opcode_call_method_and_forget();
    } else if (frame.first == static_cast<uint8_t>(Opcode::CALL_FUTURE)) {
      ctx->handle_opcode_call_future();
    } else if (frame.first == static_cast<uint8_t>(Opcode::CALL_METHOD)) {
      ctx->handle_opcode_call_method();
    } else if (frame.first == static_cast<uint8_t>(Opcode::CREATE_OBJECT)) {
      ctx->handle_opcode_create_object();
    } else if (frame.first == static_cast<uint8_t>(Opcode::DESTROY_OBJECT)) {
      ctx->handle_opcode_destroy_object();
    } else {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Unknown opcode in mrb_actor_step");
    }
    return mrb_true_value();
  }

  void
  run_actor(mrb_value actor_classes_msg, mrb_bool full_vm)
  {
    if (full_vm) {
      mrb = mrb_open();
    } else {
      mrb = mrb_open_core();
      mrb_actor_register_msgpack_extensions(mrb);
    }
    if (unlikely(!mrb)) {
      throw std::runtime_error("Failed to create mruby state in actor thread");
    }

    pair_socket.emplace(mrb, ZMQ_PAIR);
    pair_socket->connect(endpoint.c_str());
    mrb_value mrb_actor_objects = mrb_hash_new(mrb);
    mrb_gv_set(mrb, MRB_SYM(__mrb_actor_objects__), mrb_actor_objects);

    mrb_value env = mrb_cptr_value(mrb, this);
    mrb_value cfunc = mrb_obj_value(mrb_proc_new_cfunc_with_env(
        mrb,
        mrb_actor_step,
        1,
        &env
    ));

    mrb_value running = mrb_true_value();
    int idx = mrb_gc_arena_save(mrb);
    Opcode::send(*pair_socket, Opcode::READY);
    do {
      running = mrb_yield_argv(mrb, cfunc, 0, nullptr);
      mrb_gc_arena_restore(mrb, idx);
    } while (likely(mrb_true_p(running)));
    if (!full_vm) {
      mrb_msgpack_teardown(mrb);
    }

    if (unlikely(mrb->exc)) {
      mrb_exc_raise(mrb, mrb_obj_value(mrb->exc));
    }
  }
};

class mrb_inproc_actor_mailbox
{
  protected:
  mrb_state *owner_mrb = nullptr;
  mrb_value self;
  std::thread thread;
  std::string pair_endpoint;
  ZmqSocket owner_pair_socket;
  mrb_int seq_num = MRB_INT_MIN;

  std::optional<InprocActorThreadContext> ctx;

  std::string
  make_mailbox_pair_endpoint()
  {
    std::ostringstream oss;
    oss << "inproc://mailbox-" << this;
    return oss.str();
  }

  public:

  mrb_inproc_actor_mailbox(mrb_state *owner_mrb, mrb_value self) : owner_mrb(owner_mrb), self(self), owner_pair_socket(owner_mrb, ZMQ_PAIR)
  {
    mrb_state *mrb = owner_mrb;
    pair_endpoint = make_mailbox_pair_endpoint();

    owner_pair_socket.bind(pair_endpoint.c_str());
    mrb_value future_answers = mrb_hash_new(mrb);
    mrb_iv_set(mrb, self, MRB_SYM(__future_answers__), future_answers);

    // Create actor thread context
    ctx.emplace(pair_endpoint);
  }

  ~mrb_inproc_actor_mailbox()
  {
    if (thread.joinable()) {
      Opcode::send(owner_pair_socket, Opcode::SHUTDOWN);
      thread.join();
    }
  }

  void run(mrb_value actor_classes, mrb_bool full_vm)
  {
    mrb_state *mrb = owner_mrb;
    mrb_value actor_classes_msg = mrb_msgpack_pack(mrb, actor_classes);
    thread = std::thread([this, actor_classes_msg, full_vm] {
      ctx->run_actor(actor_classes_msg, full_vm);
    });

    std::pair <uint8_t, bool> frame = Opcode::recv(owner_pair_socket);
    if (unlikely(frame.first != static_cast<uint8_t>(Opcode::READY))) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Failed to receive READY from actor thread");
    }
  }

  mrb_value
  create_object(struct RClass *klass,
                         mrb_value *argv, mrb_int argc)
  {
    mrb_state *mrb = owner_mrb;
    mrb_class_path(mrb, klass);
    mrb_sym nsym = MRB_SYM(__classname__);
    mrb_value path = mrb_obj_iv_get(mrb, (struct RObject*) klass, nsym);
    if(unlikely(!mrb_symbol_p(path))) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Failed to get class path");
    }
    Opcode::send(owner_pair_socket, Opcode::CREATE_OBJECT, ZMQ_SNDMORE);
    ZmqMessage seq_msg(mrb, &++seq_num, sizeof(seq_num));
    seq_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    mrb_sym class_id = mrb_symbol(path);
    ZmqMessage class_id_msg(mrb, &class_id, sizeof(class_id));
    class_id_msg.send(owner_pair_socket, argc > 0 ? ZMQ_SNDMORE : 0);

    // Optional args frame
    if (argc > 0) {
      mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
      ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
      args_msg.send(owner_pair_socket);
    }

    return await_response_sync(
      static_cast<uint8_t>(Opcode::OBJECT_CREATED),
      seq_num
    );
  }

  mrb_value
  send(mrb_int object_id, mrb_sym method_id,
                     mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::CALL_METHOD, ZMQ_SNDMORE);
    ZmqMessage seq_msg(mrb, &++seq_num, sizeof(seq_num));
    seq_msg.send(owner_pair_socket, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    ZmqMessage method_id_msg(mrb, &method_id, sizeof(method_id));
    method_id_msg.send(owner_pair_socket, argc > 0 ? ZMQ_SNDMORE : 0);

    if (argc > 0) {
      mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
      ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
      args_msg.send(owner_pair_socket, mrb_proc_p(blk) ? ZMQ_SNDMORE : 0);
    }

    if (mrb_proc_p(blk)) {
      mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
      ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
      blk_msg.send(owner_pair_socket);
    }

    return await_response_sync(
      static_cast<uint8_t>(Opcode::RETURN_VALUE),
      seq_num
    );
  }

  void
  send_and_forget(mrb_int object_id, mrb_sym method_id,
                     mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::CALL_METHOD_AND_FORGET, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    ZmqMessage method_id_msg(mrb, &method_id, sizeof(method_id));
    method_id_msg.send(owner_pair_socket, argc > 0 ? ZMQ_SNDMORE : 0);

    if (argc > 0) {
      mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
      ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
      args_msg.send(owner_pair_socket, mrb_proc_p(blk) ? ZMQ_SNDMORE: 0);
    }

    if (mrb_proc_p(blk)) {
      mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
      ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
      blk_msg.send(owner_pair_socket);
    }

  }

  void
  call_future(mrb_int object_id, mrb_sym method_id,
                     mrb_value *argv, mrb_int argc, mrb_value blk)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::CALL_FUTURE, ZMQ_SNDMORE);
    ZmqMessage seq_msg(mrb, &++seq_num, sizeof(seq_num));
    mrb_value future_answers = mrb_iv_get(mrb, self, MRB_SYM(__future_answers__));
    mrb_assert(mrb_hash_p(future_answers));
    mrb_value seq_key = mrb_convert_number(mrb, seq_num);
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    mrb_value pair = mrb_assoc_new(mrb, mrb_convert_number(mrb, Opcode::TIMEOUT), mrb_convert_number(mrb, timeout.time_since_epoch().count()));
    mrb_hash_set(mrb, future_answers, seq_key, pair);
    seq_msg.send(owner_pair_socket, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket, ZMQ_SNDMORE);

    ZmqMessage method_id_msg(mrb, &method_id, sizeof(method_id));
    method_id_msg.send(owner_pair_socket, argc > 0 ? ZMQ_SNDMORE : 0);

    if (argc > 0) {
      mrb_value packed_args = mrb_msgpack_pack_argv(mrb, argv, argc);
      ZmqMessage args_msg(mrb, RSTRING_PTR(packed_args), RSTRING_LEN(packed_args));
      args_msg.send(owner_pair_socket, mrb_proc_p(blk) ? ZMQ_SNDMORE : 0);
    }

    if (mrb_proc_p(blk)) {
      mrb_value packed_blk = mrb_msgpack_pack(mrb, blk);
      ZmqMessage blk_msg(mrb, RSTRING_PTR(packed_blk), RSTRING_LEN(packed_blk));
      blk_msg.send(owner_pair_socket);
    }
  }

  void
  destroy_object(mrb_int object_id)
  {
    mrb_state *mrb = owner_mrb;
    Opcode::send(owner_pair_socket, Opcode::DESTROY_OBJECT, ZMQ_SNDMORE);
    ZmqMessage obj_id_msg(mrb, &object_id, sizeof(object_id));
    obj_id_msg.send(owner_pair_socket);
  }

  protected:
  void
  handle_futures(uint8_t opcode)
  {
    mrb_state *mrb = owner_mrb;
    ZmqMessage seq_msg(mrb);
    int rc = seq_msg.recv(owner_pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed sequence number in future response");
    }
    mrb_int resp_seq_num =
      *static_cast<const mrb_int*>(seq_msg.data());

    ZmqMessage payload_msg(mrb);
    payload_msg.recv(owner_pair_socket);
    mrb_value payload_str = mrb_str_new_static(mrb,
      static_cast<const char*>(payload_msg.data()),
      payload_msg.size()
    );
    mrb_value payload = mrb_msgpack_unpack(mrb, payload_str);

    mrb_value future_answers = mrb_iv_get(mrb, self, MRB_SYM(__future_answers__));
    mrb_assert(mrb_hash_p(future_answers));
    mrb_value seq_key = mrb_convert_number(mrb, resp_seq_num);
    mrb_value pair = mrb_assoc_new(mrb,
      mrb_convert_number(mrb, opcode),
      payload
    );
    mrb_hash_set(mrb, future_answers, seq_key, pair);
  }

  mrb_value
  handle_sync_calls(uint8_t expected_type_class, mrb_int expected_seq_num)
  {
    mrb_state *mrb = owner_mrb;
    ZmqMessage seq_msg(mrb);
    int rc = seq_msg.recv(owner_pair_socket);
    if (unlikely(rc != sizeof(mrb_int))) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed sequence number in response");
    }
    mrb_int resp_seq_num =
      *static_cast<const mrb_int*>(seq_msg.data());
    if (unlikely(resp_seq_num != expected_seq_num)) {
      mrb_raise(mrb, E_RUNTIME_ERROR, "Mismatched sequence number in response");
    }

    switch (expected_type_class) {
      case static_cast<uint8_t>(Opcode::RETURN_VALUE): {
        ZmqMessage ret_msg(mrb);
        ret_msg.recv(owner_pair_socket);
        mrb_value ret_str = mrb_str_new_static(mrb,
          static_cast<const char*>(ret_msg.data()),
          ret_msg.size()
        );
        return mrb_msgpack_unpack(mrb, ret_str);
      }
      case static_cast<uint8_t>(Opcode::OBJECT_CREATED): {
        ZmqMessage obj_id_msg(mrb);
        obj_id_msg.recv(owner_pair_socket);
        if (unlikely(obj_id_msg.size() != sizeof(mrb_int))) {
          mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed OBJECT_CREATED object ID in handle_sync_calls");
        }
        return mrb_convert_number(mrb, *static_cast<const mrb_int*>(obj_id_msg.data()));
      }
      default:
        mrb_raise(mrb, E_RUNTIME_ERROR, "Unexpected response type in handle_sync_calls");
    }
  }

  mrb_value
  await_response_sync(uint8_t expected_type_class, mrb_int expected_seq_num)
  {
    mrb_state *mrb = owner_mrb;
    while (true) {
      std::pair <uint8_t, bool> frame = Opcode::recv(owner_pair_socket);
      if (frame.first == static_cast<uint8_t>(Opcode::FUTURE_VALUE) ||
          frame.first == static_cast<uint8_t>(Opcode::FUTURE_EXCEPTION)) {
        handle_futures(frame.first);
      } else if (frame.first == expected_type_class) {
        return handle_sync_calls(expected_type_class, expected_seq_num);
      } else if (unlikely(frame.first == static_cast<uint8_t>(Opcode::RAISE_EXCEPTION))) {
        ZmqMessage seq_msg(mrb);
        int rc = seq_msg.recv(owner_pair_socket);
        if (unlikely(rc != sizeof(mrb_int))) {
          mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed sequence number in response");
        }
        mrb_int resp_seq_num =
          *static_cast<const mrb_int*>(seq_msg.data());
        if (unlikely(resp_seq_num != expected_seq_num)) {
          mrb_raise(mrb, E_RUNTIME_ERROR, "Mismatched sequence number in response");
        }

        ZmqMessage exc_msg(mrb);
        exc_msg.recv(owner_pair_socket);
        mrb_value exc_str = mrb_str_new_static(mrb,
          static_cast<const char*>(exc_msg.data()),
          exc_msg.size()
        );
        mrb_value exc = mrb_msgpack_unpack(mrb, exc_str);
        mrb_exc_raise(mrb, exc);
      } else {
        mrb_raise(mrb, E_RUNTIME_ERROR, "Malformed response opcode in await_response_sync");
      }
    }
    return mrb_undef_value(); // Unreachable
  }
};

MRB_CPP_DEFINE_TYPE(mrb_inproc_actor_mailbox, mrb_inproc_actor_mailbox);

static mrb_value
mrb_actor_inproc_new(mrb_state* mrb, mrb_value self)
{
#ifdef MRB_NO_PRESYM
  mrb_raise(mrb, E_RUNTIME_ERROR,
            "mruby/presym is required for mruby-actor inproc actors");
#else
  // --- keyword argument setup ---
  mrb_sym kw_names[] = { MRB_SYM(full_vm) };
  mrb_value kw_values[1];
  mrb_kwargs kwargs = {
    1,      // num keywords
    0,      // required keywords
    kw_names,
    kw_values,
    NULL    // no **rest allowed
  };

  // Parse only keyword args
  mrb_get_args(mrb, ":", &kwargs);

  // Default
  mrb_bool full_vm = FALSE;

  // If provided, override
  if (!mrb_undef_p(kw_values[0])) {
    full_vm = mrb_test(kw_values[0]);
  }

  struct RClass *actorizable_mod =
      mrb_module_get_id(mrb, MRB_SYM(Actorizable));

  mrb_inproc_actor_mailbox* mailbox =
      mrb_cpp_new<mrb_inproc_actor_mailbox>(mrb, self, mrb, self);

  mrb_value actor_classes =
      mrb_iv_get(mrb, mrb_obj_value(actorizable_mod),
                 MRB_IVSYM(actor_classes));

  mailbox->run(actor_classes, full_vm);
#endif

  return self;
}



struct mrb_actor_object {
  mrb_inproc_actor_mailbox* mailbox;
  mrb_int object_id;

  mrb_actor_object(mrb_inproc_actor_mailbox* mailbox, mrb_int object_id)
    : mailbox(mailbox), object_id(object_id) {}

  ~mrb_actor_object()
  {
    mailbox->destroy_object(object_id);
  }
};

MRB_CPP_DEFINE_TYPE(mrb_actor_object, mrb_actor_object);

static mrb_value
mrb_actor_object_initialize(mrb_state* mrb, mrb_value self)
{
  mrb_inproc_actor_mailbox* mailbox = nullptr;
  mrb_int object_id = 0;
  mrb_get_args(mrb, "di", &mailbox, &mrb_inproc_actor_mailbox_type, &object_id);
  mrb_cpp_new<mrb_actor_object>(mrb, self, mailbox, object_id);

  return self;
}

static mrb_value
mrb_actor_object_send(mrb_state* mrb, mrb_value self)
{
  mrb_actor_object* actor_obj = static_cast<mrb_actor_object*>(
    mrb_data_get_ptr(mrb, self, &mrb_actor_object_type)
  );

  mrb_sym method_id;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;
  mrb_value blk = mrb_nil_value();
  mrb_get_args(mrb, "n|*&", &method_id, &argv, &argc, &blk);

  return actor_obj->mailbox->send(
    actor_obj->object_id, method_id, argv, argc, blk
  );
}

static mrb_value
mrb_actor_object_send_and_forget(mrb_state* mrb, mrb_value self)
{
  mrb_actor_object* actor_obj = static_cast<mrb_actor_object*>(
    mrb_data_get_ptr(mrb, self, &mrb_actor_object_type)
  );
  mrb_sym method_id;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;
  mrb_value blk = mrb_nil_value();
  mrb_get_args(mrb, "n|*&", &method_id, &argv, &argc, &blk);
  actor_obj->mailbox->send_and_forget(
    actor_obj->object_id, method_id, argv, argc, blk
  );
  return mrb_nil_value();
}

static mrb_value
mrb_actor_inproc_create_object(mrb_state* mrb, mrb_value self)
{
  mrb_inproc_actor_mailbox* mailbox = static_cast<mrb_inproc_actor_mailbox*>(
    mrb_data_get_ptr(mrb, self, &mrb_inproc_actor_mailbox_type)
  );

  struct RClass* klass = nullptr;
  mrb_value *argv = nullptr;
  mrb_int argc = 0;

  mrb_get_args(mrb, "c|*", &klass, &argv, &argc);

  mrb_value object_id = mailbox->create_object(klass, argv, argc);
  return mrb_obj_new(mrb, mrb_class_get_under_id(mrb, mrb_class(mrb, self), MRB_SYM(ActorObject)), 2,
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

  struct RClass *actor_class, *actor_inproc_class, *actor_object_class;
  actor_class = mrb_define_class_id(mrb, MRB_SYM(Actor), mrb->object_class);
  MRB_SET_INSTANCE_TT(actor_class, MRB_TT_DATA);
  actor_object_class = mrb_define_class_under_id(mrb, actor_class, MRB_SYM(ActorObject), mrb->object_class);
  MRB_SET_INSTANCE_TT(actor_object_class, MRB_TT_DATA);
  mrb_define_method_id(mrb, actor_object_class, MRB_SYM(initialize), mrb_actor_object_initialize,
    MRB_ARGS_REQ(2));
  mrb_define_method_id(mrb, actor_object_class, MRB_SYM(send), mrb_actor_object_send,
    MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK()|MRB_ARGS_REST());
  mrb_define_method_id(mrb, actor_object_class, MRB_SYM(send_and_forget), mrb_actor_object_send_and_forget,
    MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK()|MRB_ARGS_REST());
  actor_inproc_class = mrb_define_class_id(mrb, MRB_SYM(InprocActor), actor_class);
  mrb_define_method_id(mrb, actor_inproc_class, MRB_SYM(initialize), mrb_actor_inproc_new, MRB_ARGS_KEY(1, 0));
  mrb_define_method_id(mrb, actor_inproc_class, MRB_SYM(new), mrb_actor_inproc_create_object,
    MRB_ARGS_REQ(1)|MRB_ARGS_REST());

  mrb_actor_register_msgpack_extensions(mrb);
}

void
mrb_mruby_actor_gem_final(mrb_state* mrb) {}
MRB_END_DECL
