#pragma once
#include <mruby.h>
#include <mruby/proc.h>
#include <mruby/array.h>
#include <mruby/class.h>
#include <mruby/presym.h>
#include <mruby/msgpack.h>
#include <mruby/value.h>
#include <mruby/branch_pred.h>
#include <mruby/string.h>
#include <mruby/proc_irep_ext.h>

static mrb_value
pack_exception(mrb_state *mrb, mrb_value self)
{
  mrb_value exc;
  mrb_get_args(mrb, "o", &exc);
  if (unlikely(!mrb_exception_p(exc))) {
    mrb_raise(mrb, E_TYPE_ERROR, "not a Exception object");
    return mrb_undef_value();
  }
  mrb_value ary = mrb_ary_new_capa(mrb, 3);

  // exc.class.name
  mrb_value name  = mrb_class_path(mrb, mrb_obj_class(mrb, exc));
  mrb_ary_push(mrb, ary, name);

  // exc.message
  mrb_value msg = mrb_funcall_argv(mrb, exc, MRB_SYM(message), 0, NULL);
  mrb_ary_push(mrb, ary, msg);

  // exc.backtrace
  mrb_value bt = mrb_funcall_argv(mrb, exc, MRB_SYM(backtrace), 0, NULL);
  mrb_ary_push(mrb, ary, bt);

  return mrb_msgpack_pack(mrb, ary);
}

static mrb_value
unpack_exception(mrb_state *mrb, mrb_value self)
{
  mrb_value data;
  mrb_get_args(mrb, "S", &data);
  mrb_value ary = mrb_msgpack_unpack(mrb, data);
  if (unlikely(!mrb_array_p(ary))) {
    mrb_raise(mrb, E_TYPE_ERROR, "expected an Array");
    return mrb_undef_value();
  }

  mrb_value class_name = mrb_ary_ref(mrb, ary, 0);
  mrb_value message    = mrb_ary_ref(mrb, ary, 1);
  mrb_value backtrace  = mrb_ary_ref(mrb, ary, 2);

  mrb_value exc_class = mrb_str_constantize(mrb, class_name);

  mrb_value exc;
  if (mrb_string_p(message) && RSTRING_LEN(message) > 0) {
    exc = mrb_obj_new(mrb, mrb_class_ptr(exc_class), 1, &message);
  } else {
    exc = mrb_obj_new(mrb, mrb_class_ptr(exc_class), 0, NULL);
  }
  if (unlikely(!mrb_exception_p(exc))) {
    mrb_raise(mrb, E_TYPE_ERROR, "Requested class isn't a Exception subclass");
    return mrb_undef_value();
  }

  if (mrb_array_p(backtrace) && RARRAY_LEN(backtrace) > 0) {
    mrb_funcall_argv(mrb, exc, MRB_SYM(set_backtrace), 1, &backtrace);
  }

  return exc;
}

static mrb_value
pack_proc(mrb_state *mrb, mrb_value self)
{
  mrb_value proc;
  mrb_get_args(mrb, "o", &proc);

  if(unlikely(!mrb_proc_p(proc))) {
    mrb_raise(mrb, E_TYPE_ERROR, "expected a Proc");
    return mrb_undef_value();
  }

  return mrb_proc_to_irep(mrb, mrb_proc_ptr(proc));
}

static mrb_value
unpack_proc(mrb_state *mrb, mrb_value self)
{
  mrb_value irep;
  mrb_get_args(mrb, "S", &irep);
  mrb_str_modify(mrb, mrb_str_ptr(irep));

  return mrb_proc_from_irep(mrb, RSTRING_PTR(irep), RSTRING_LEN(irep));
}

static mrb_value
pack_class(mrb_state *mrb, mrb_value self)
{
  mrb_value klass;
  mrb_get_args(mrb, "C", &klass);
  return mrb_class_path(mrb, mrb_class_ptr(klass));
}

static mrb_value
unpack_class(mrb_state *mrb, mrb_value self)
{
  mrb_value data;
  mrb_get_args(mrb, "S", &data);
  return mrb_str_constantize(mrb, data);
}

static void
mrb_actor_register_msgpack_extensions(mrb_state *mrb)
{
  mrb_msgpack_ensure(mrb);
  mrb_msgpack_set_symbol_strategy(mrb, MRB_SYM(int), 0);

  mrb_msgpack_register_pack_type_cfunc(
    mrb, 10, mrb->eException_class,
    pack_exception,
    0, NULL
  );

  mrb_msgpack_register_unpack_type_cfunc(
    mrb, 10,
    unpack_exception,
    0, NULL
  );

  mrb_msgpack_register_pack_type_cfunc(
    mrb, 11, mrb->proc_class,
    pack_proc,
    0, NULL
  );

  mrb_msgpack_register_unpack_type_cfunc(
    mrb, 11,
    unpack_proc,
    0, NULL
  );

  mrb_msgpack_register_pack_type_cfunc(
    mrb, 12, mrb->class_class,
    pack_class,
    0, NULL
  );

  mrb_msgpack_register_unpack_type_cfunc(
    mrb, 12,
    unpack_class,
    0, NULL
  );
}