#include <mruby.h>
#include <mruby/array.h>
#include <mruby/cbor.h>
#include <mruby/class.h>
#include <mruby/cpp_helpers.hpp>
#include <mruby/data.h>
#include <mruby/error.h>
#include <mruby/hash.h>
#include <mruby/presym.h>
#include <mruby/string.h>
#include <mruby/str_constantize.h>
#include <mruby/value.h>
#include <mruby/variable.h>

#include "mpsc_mailbox.hpp"

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace {

enum class Op : uint8_t {
  CREATE          = 1,
  DESTROY         = 2,
  CALL            = 3,
  CALL_AND_FORGET = 4,
  SHUTDOWN        = 255,
};

enum class ReplyKind : uint8_t {
  VALUE     = 1,
  EXCEPTION = 2,
};

struct Reply {
  ReplyKind   kind = ReplyKind::VALUE;
  std::string payload;
};

struct Envelope {
  Op                                   op = Op::SHUTDOWN;
  std::string                          payload;
  std::shared_ptr<std::promise<Reply>> reply;
};

// Mailbox: lock-free MPSC queue of Envelopes plus a tiny shutdown
// handshake. The fast path (push/pop) never grabs a lock — producers
// race only on a single atomic exchange and notify via std::atomic::wait.
//
// Shutdown is coordinated with an atomic `closed_` flag and an `inflight_`
// counter (Dekker-style with seq_cst on the critical pair). Once
// close_and_drain() returns, every push that has not yet incremented
// `inflight_` is guaranteed to short-circuit with the canned reply, and
// every push already past that point has been observed and either
// enqueued (then drained) or skipped.
class Mailbox {
public:
  void push(Envelope env) {
    inflight_.fetch_add(1, std::memory_order_seq_cst);
    if (closed_.load(std::memory_order_seq_cst)) {
      inflight_.fetch_sub(1, std::memory_order_release);
      reply_canned(env);
      return;
    }
    queue_.push(std::move(env));
    inflight_.fetch_sub(1, std::memory_order_release);
  }

  Envelope pop() { return queue_.pop_blocking(); }

  bool closed() const {
    return closed_.load(std::memory_order_acquire);
  }

  // Called by the actor thread (single producer of the close event)
  // *after* it has exited its own loop. Synchronises with all in-flight
  // pushers, then drains anything still in the queue, replying with the
  // canned exception so no caller ever blocks on a future forever.
  void close_and_drain(std::string canned_exception_payload) {
    canned_payload_ = std::move(canned_exception_payload);
    closed_.store(true, std::memory_order_seq_cst);
    while (inflight_.load(std::memory_order_seq_cst) > 0) {
      std::this_thread::yield();
    }
    Envelope e;
    while (queue_.try_pop(e)) {
      reply_canned(e);
    }
    // Wake any consumers parked in pop() so they can observe shutdown.
    queue_.wake_all();
  }

private:
  void reply_canned(Envelope& env) {
    if (env.reply) {
      Reply r;
      r.kind    = ReplyKind::EXCEPTION;
      r.payload = canned_payload_;
      env.reply->set_value(std::move(r));
    }
  }

  MpscQueue<Envelope>   queue_;
  std::atomic<bool>     closed_{false};
  std::atomic<uint32_t> inflight_{0};
  std::string           canned_payload_;
};

// ---------- Actor-thread side ----------

static mrb_value actor_object_table(mrb_state* mrb) {
  mrb_value t = mrb_gv_get(mrb, MRB_SYM(__mrb_actor_objects__));
  if (!mrb_hash_p(t)) {
    t = mrb_hash_new(mrb);
    mrb_gv_set(mrb, MRB_SYM(__mrb_actor_objects__), t);
  }
  return t;
}

static std::string encode_value(mrb_state* mrb, mrb_value v) {
  mrb_value bytes = mrb_cbor_encode(mrb, v);
  return std::string(RSTRING_PTR(bytes), RSTRING_LEN(bytes));
}

static mrb_value decode_value(mrb_state* mrb, const std::string& s) {
  mrb_value buf = mrb_str_new(mrb, s.data(), s.size());
  return mrb_cbor_decode(mrb, buf);
}

static std::string encode_exception(mrb_state* mrb, mrb_value exc) {
  mrb_value ary  = mrb_ary_new_capa(mrb, 3);
  mrb_value path = mrb_class_path(mrb, mrb_obj_class(mrb, exc));
  mrb_ary_push(mrb, ary, path);
  mrb_ary_push(mrb, ary, mrb_funcall_argv(mrb, exc, MRB_SYM(message), 0, nullptr));
  mrb_ary_push(mrb, ary, mrb_funcall_argv(mrb, exc, MRB_SYM(backtrace), 0, nullptr));
  return encode_value(mrb, ary);
}

[[noreturn]] static void raise_decoded_exception(mrb_state* mrb, const std::string& payload) {
  mrb_value ary = decode_value(mrb, payload);
  if (!mrb_array_p(ary) || RARRAY_LEN(ary) < 2) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "actor returned malformed exception payload");
  }
  mrb_value path      = mrb_ary_ref(mrb, ary, 0);
  mrb_value message   = mrb_ary_ref(mrb, ary, 1);
  mrb_value backtrace = RARRAY_LEN(ary) > 2 ? mrb_ary_ref(mrb, ary, 2) : mrb_nil_value();

  struct RClass* cls = mrb_class_get_id(mrb, MRB_SYM(RuntimeError));
  if (mrb_string_p(path)) {
    mrb_value resolved = mrb_str_constantize(mrb, path);
    if (mrb_class_p(resolved)) {
      cls = mrb_class_ptr(resolved);
    }
  }

  mrb_value exc;
  if (mrb_string_p(message) && RSTRING_LEN(message) > 0) {
    exc = mrb_obj_new(mrb, cls, 1, &message);
  } else {
    exc = mrb_obj_new(mrb, cls, 0, nullptr);
  }
  if (mrb_array_p(backtrace) && RARRAY_LEN(backtrace) > 0) {
    mrb_funcall_argv(mrb, exc, MRB_SYM(set_backtrace), 1, &backtrace);
  }
  mrb_exc_raise(mrb, exc);
}

// CREATE payload: [class_path_string, args_array]
static std::string handle_create(mrb_state* mrb, const std::string& payload) {
  mrb_value req = decode_value(mrb, payload);
  if (!mrb_array_p(req) || RARRAY_LEN(req) != 2) {
    mrb_raise(mrb, E_ARGUMENT_ERROR, "malformed CREATE request");
  }
  mrb_value class_path = mrb_ary_ref(mrb, req, 0);
  mrb_value args       = mrb_ary_ref(mrb, req, 1);
  if (!mrb_string_p(class_path)) {
    mrb_raise(mrb, E_TYPE_ERROR, "class path must be a String");
  }
  mrb_value resolved = mrb_str_constantize(mrb, class_path);
  if (!mrb_class_p(resolved)) {
    mrb_raisef(mrb, E_RUNTIME_ERROR, "no such class %!v in actor mrb", class_path);
  }

  mrb_value obj;
  if (mrb_array_p(args)) {
    obj = mrb_obj_new(mrb, mrb_class_ptr(resolved),
                      RARRAY_LEN(args), RARRAY_PTR(args));
  } else {
    obj = mrb_obj_new(mrb, mrb_class_ptr(resolved), 0, nullptr);
  }

  mrb_value table = actor_object_table(mrb);
  mrb_int   oid   = mrb_obj_id(obj);
  mrb_hash_set(mrb, table, mrb_int_value(mrb, oid), obj);
  return encode_value(mrb, mrb_int_value(mrb, oid));
}

// CALL / CALL_AND_FORGET payload: [object_id, method, args_array]
static std::string handle_call(mrb_state* mrb, const std::string& payload) {
  mrb_value req = decode_value(mrb, payload);
  if (!mrb_array_p(req) || RARRAY_LEN(req) != 3) {
    mrb_raise(mrb, E_ARGUMENT_ERROR, "malformed CALL request");
  }
  mrb_value oid    = mrb_ary_ref(mrb, req, 0);
  mrb_value method = mrb_ary_ref(mrb, req, 1);
  mrb_value args   = mrb_ary_ref(mrb, req, 2);

  mrb_value table  = actor_object_table(mrb);
  mrb_value target = mrb_hash_fetch(mrb, table, oid, mrb_undef_value());
  if (mrb_undef_p(target)) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "unknown actor object_id");
  }

  mrb_sym mid;
  if (mrb_symbol_p(method)) {
    mid = mrb_symbol(method);
  } else if (mrb_string_p(method)) {
    mid = mrb_intern_str(mrb, method);
  } else {
    mrb_raise(mrb, E_TYPE_ERROR, "method name must be Symbol or String");
  }

  mrb_value ret;
  if (mrb_array_p(args)) {
    ret = mrb_funcall_argv(mrb, target, mid, RARRAY_LEN(args), RARRAY_PTR(args));
  } else {
    ret = mrb_funcall_argv(mrb, target, mid, 0, nullptr);
  }
  return encode_value(mrb, ret);
}

static void handle_destroy(mrb_state* mrb, const std::string& payload) {
  mrb_value oid   = decode_value(mrb, payload);
  mrb_value table = actor_object_table(mrb);
  mrb_hash_delete_key(mrb, table, oid);
}

struct StepCtx {
  Envelope* env;
  Reply*    reply;
};

static mrb_value step_protected(mrb_state* mrb, void* data) {
  StepCtx*  ctx   = static_cast<StepCtx*>(data);
  Envelope& env   = *ctx->env;
  Reply&    reply = *ctx->reply;
  switch (env.op) {
    case Op::CREATE:
      reply.payload = handle_create(mrb, env.payload);
      break;
    case Op::CALL:
    case Op::CALL_AND_FORGET:
      reply.payload = handle_call(mrb, env.payload);
      break;
    case Op::DESTROY:
      handle_destroy(mrb, env.payload);
      break;
    default:
      break;
  }
  return mrb_nil_value();
}

static void run_actor(std::shared_ptr<Mailbox> mb,
                      std::shared_ptr<std::promise<bool>> ready) {
  mrb_state* mrb = mrb_open();
  if (!mrb) {
    ready->set_value(false);
    mb->close_and_drain({});
    return;
  }
  ready->set_value(true);

  while (true) {
    Envelope env = mb->pop();
    if (env.op == Op::SHUTDOWN) {
      break;
    }

    Reply   reply;
    int     ai  = mrb_gc_arena_save(mrb);
    mrb_bool err = FALSE;
    StepCtx ctx{&env, &reply};
    mrb_value protected_ret =
        mrb_protect_error(mrb, step_protected, &ctx, &err);
    if (err) {
      reply.kind = ReplyKind::EXCEPTION;
      mrb_bool enc_err = FALSE;
      struct EncCtx { mrb_value exc; std::string out; };
      EncCtx ectx{protected_ret, {}};
      mrb_protect_error(
          mrb,
          [](mrb_state* m, void* d) -> mrb_value {
            EncCtx* e = static_cast<EncCtx*>(d);
            e->out = encode_exception(m, e->exc);
            return mrb_nil_value();
          },
          &ectx, &enc_err);
      if (enc_err) {
        // Fallback: hand-build a minimal payload so the caller still gets *something*.
        mrb_value ary = mrb_ary_new_capa(mrb, 3);
        mrb_ary_push(mrb, ary, mrb_str_new_lit(mrb, "RuntimeError"));
        mrb_ary_push(mrb, ary, mrb_str_new_lit(mrb, "actor failed and exception could not be serialised"));
        mrb_ary_push(mrb, ary, mrb_nil_value());
        reply.payload = encode_value(mrb, ary);
      } else {
        reply.payload = std::move(ectx.out);
      }
    }
    mrb_gc_arena_restore(mrb, ai);

    if (env.reply) {
      env.reply->set_value(std::move(reply));
    }
  }

  // Synthesize a "actor shut down" exception payload so any messages
  // pushed after shutdown can be answered with a sensible exception.
  mrb_value canned_path    = mrb_str_new_lit(mrb, "RuntimeError");
  mrb_value canned_message = mrb_str_new_lit(mrb, "actor has shut down");
  mrb_value canned_ary     = mrb_ary_new_capa(mrb, 3);
  mrb_ary_push(mrb, canned_ary, canned_path);
  mrb_ary_push(mrb, canned_ary, canned_message);
  mrb_ary_push(mrb, canned_ary, mrb_nil_value());
  std::string canned = encode_value(mrb, canned_ary);

  mb->close_and_drain(std::move(canned));
  mrb_close(mrb);
}

// ---------- Owner-side wrappers ----------

struct ActorWrapper {
  std::shared_ptr<Mailbox> mb;
  std::thread              thread;

  ActorWrapper() : mb(std::make_shared<Mailbox>()) {
    auto ready = std::make_shared<std::promise<bool>>();
    auto fut   = ready->get_future();
    auto mb_   = mb;
    thread = std::thread([mb_, ready] { run_actor(mb_, ready); });
    if (!fut.get()) {
      if (thread.joinable()) {
        thread.join();
      }
      throw std::runtime_error("Failed to open mruby state in actor thread");
    }
  }

  ~ActorWrapper() {
    if (!thread.joinable()) return;
    Envelope env;
    env.op = Op::SHUTDOWN;
    mb->push(std::move(env));
    thread.join();
  }
};

struct ActorObject {
  std::shared_ptr<Mailbox> mb;
  mrb_int                  object_id;
  std::string              oid_cbor;

  ActorObject(std::shared_ptr<Mailbox> mb_, mrb_int oid, std::string cbor)
    : mb(std::move(mb_)), object_id(oid), oid_cbor(std::move(cbor)) {}

  ~ActorObject() {
    if (mb->closed()) return;
    Envelope env;
    env.op      = Op::DESTROY;
    env.payload = oid_cbor;
    mb->push(std::move(env));
  }
};

} // namespace

MRB_CPP_DEFINE_TYPE(ActorWrapper, mrb_actor_wrapper)
MRB_CPP_DEFINE_TYPE(ActorObject,  mrb_actor_object)

namespace {

static Reply send_and_wait(mrb_state* mrb, const std::shared_ptr<Mailbox>& mb,
                           Op op, std::string payload) {
  Envelope env;
  env.op      = op;
  env.payload = std::move(payload);
  env.reply   = std::make_shared<std::promise<Reply>>();
  auto fut    = env.reply->get_future();
  mb->push(std::move(env));
  return fut.get();
}

static mrb_value mrb_actor_initialize(mrb_state* mrb, mrb_value self) {
  try {
    mrb_cpp_new<ActorWrapper>(mrb, self);
  } catch (const std::exception& e) {
    mrb_raise(mrb, E_RUNTIME_ERROR, e.what());
  }
  return self;
}

static mrb_value build_actor_object(mrb_state* mrb, mrb_value actor_self,
                                    std::shared_ptr<Mailbox> mb,
                                    mrb_int oid, std::string oid_cbor) {
  struct RClass* root = mrb_class_get_id(mrb, MRB_SYM(Actor));
  struct RClass* cls  = mrb_class_get_under_id(mrb, root, MRB_SYM(Object));
  mrb_value obj = mrb_obj_value(mrb_data_object_alloc(mrb, cls, nullptr, nullptr));
  mrb_cpp_new<ActorObject>(mrb, obj, std::move(mb), oid, std::move(oid_cbor));
  mrb_iv_set(mrb, obj, MRB_SYM(__actor__), actor_self);
  return obj;
}

static mrb_value mrb_actor_create(mrb_state* mrb, mrb_value self) {
  ActorWrapper* w = mrb_cpp_get<ActorWrapper>(mrb, self);

  struct RClass* klass;
  mrb_value*     argv = nullptr;
  mrb_int        argc = 0;
  mrb_get_args(mrb, "c*", &klass, &argv, &argc);

  mrb_value class_path = mrb_class_path(mrb, klass);
  mrb_value args_ary   = mrb_ary_new_from_values(mrb, argc, argv);
  mrb_value req        = mrb_ary_new_capa(mrb, 2);
  mrb_ary_push(mrb, req, class_path);
  mrb_ary_push(mrb, req, args_ary);

  Reply rep = send_and_wait(mrb, w->mb, Op::CREATE, encode_value(mrb, req));
  if (rep.kind == ReplyKind::EXCEPTION) {
    raise_decoded_exception(mrb, rep.payload);
  }

  mrb_value oid_v = decode_value(mrb, rep.payload);
  if (!mrb_integer_p(oid_v)) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "actor returned non-integer object id");
  }
  mrb_int oid = mrb_integer(oid_v);

  mrb_value oid_bytes = mrb_cbor_encode(mrb, oid_v);
  std::string oid_cbor(RSTRING_PTR(oid_bytes), RSTRING_LEN(oid_bytes));

  return build_actor_object(mrb, self, w->mb, oid, std::move(oid_cbor));
}

static mrb_value mrb_actor_object_call(mrb_state* mrb, mrb_value self) {
  ActorObject* ao = mrb_cpp_get<ActorObject>(mrb, self);

  mrb_sym    method;
  mrb_value* argv = nullptr;
  mrb_int    argc = 0;
  mrb_get_args(mrb, "n*", &method, &argv, &argc);

  mrb_value args_ary = mrb_ary_new_from_values(mrb, argc, argv);
  mrb_value req      = mrb_ary_new_capa(mrb, 3);
  mrb_ary_push(mrb, req, mrb_int_value(mrb, ao->object_id));
  mrb_ary_push(mrb, req, mrb_symbol_value(method));
  mrb_ary_push(mrb, req, args_ary);

  Reply rep = send_and_wait(mrb, ao->mb, Op::CALL, encode_value(mrb, req));
  if (rep.kind == ReplyKind::EXCEPTION) {
    raise_decoded_exception(mrb, rep.payload);
  }
  return decode_value(mrb, rep.payload);
}

static mrb_value mrb_actor_object_call_and_forget(mrb_state* mrb, mrb_value self) {
  ActorObject* ao = mrb_cpp_get<ActorObject>(mrb, self);

  mrb_sym    method;
  mrb_value* argv = nullptr;
  mrb_int    argc = 0;
  mrb_get_args(mrb, "n*", &method, &argv, &argc);

  mrb_value args_ary = mrb_ary_new_from_values(mrb, argc, argv);
  mrb_value req      = mrb_ary_new_capa(mrb, 3);
  mrb_ary_push(mrb, req, mrb_int_value(mrb, ao->object_id));
  mrb_ary_push(mrb, req, mrb_symbol_value(method));
  mrb_ary_push(mrb, req, args_ary);

  Envelope env;
  env.op      = Op::CALL_AND_FORGET;
  env.payload = encode_value(mrb, req);
  ao->mb->push(std::move(env));
  return mrb_nil_value();
}

static mrb_value mrb_actor_object_id(mrb_state* mrb, mrb_value self) {
  ActorObject* ao = mrb_cpp_get<ActorObject>(mrb, self);
  return mrb_int_value(mrb, ao->object_id);
}

} // namespace

extern "C" void
mrb_mruby_actor_gem_init(mrb_state* mrb)
{
  struct RClass* actor_class =
      mrb_define_class_id(mrb, MRB_SYM(Actor), mrb->object_class);
  MRB_SET_INSTANCE_TT(actor_class, MRB_TT_DATA);

  mrb_define_method_id(mrb, actor_class, MRB_SYM(initialize),
                       mrb_actor_initialize, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, actor_class, MRB_SYM(create),
                       mrb_actor_create,
                       MRB_ARGS_REQ(1) | MRB_ARGS_REST());

  struct RClass* object_class =
      mrb_define_class_under_id(mrb, actor_class, MRB_SYM(Object),
                                mrb->object_class);
  MRB_SET_INSTANCE_TT(object_class, MRB_TT_DATA);
  mrb_undef_class_method_id(mrb, object_class, MRB_SYM(new));

  mrb_define_method_id(mrb, object_class, MRB_SYM(call),
                       mrb_actor_object_call,
                       MRB_ARGS_REQ(1) | MRB_ARGS_REST());
  mrb_define_method_id(mrb, object_class, MRB_SYM(call_and_forget),
                       mrb_actor_object_call_and_forget,
                       MRB_ARGS_REQ(1) | MRB_ARGS_REST());
  mrb_define_method_id(mrb, object_class, MRB_SYM(object_id),
                       mrb_actor_object_id, MRB_ARGS_NONE());
}

extern "C" void
mrb_mruby_actor_gem_final(mrb_state* mrb)
{
}
