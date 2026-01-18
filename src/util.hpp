#pragma once
#include <mruby.h>
#include <mruby/array.h>
#include <mruby/class.h>
#include <mruby/presym.h>
#include <mruby/msgpack.h>
#include <mruby/value.h>

static mrb_value
pack_exception(mrb_state *mrb, mrb_value self)
{
  mrb_value exc = mrb_undef_value();
  mrb_get_args(mrb, "o", &exc);
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
  mrb_value e = mrb_msgpack_pack(mrb, ary);
  return e;

}

static mrb_value
unpack_exception(mrb_state *mrb, mrb_value self)
{
  mrb_value data;
  mrb_get_args(mrb, "S", &data);
  mrb_value ary = mrb_msgpack_unpack(mrb, data);

  mrb_value class_name = mrb_ary_ref(mrb, ary, 0);
  mrb_value message    = mrb_ary_ref(mrb, ary, 1);
  mrb_value backtrace  = mrb_ary_ref(mrb, ary, 2);

  // constantize via C API
  mrb_value exc_class = mrb_str_constantize(mrb, class_name);

  // instantiate
  mrb_value exc;
  if (!mrb_nil_p(message)) {
    exc = mrb_obj_new(mrb, mrb_class_ptr(exc_class), 1, &message);
  } else {
    exc = mrb_obj_new(mrb, mrb_class_ptr(exc_class), 0, NULL);
  }

  // exc.set_backtrace(backtrace)
  if (!mrb_nil_p(backtrace)) {
    mrb_funcall_argv(mrb, exc, MRB_SYM(set_backtrace), 1, &backtrace);
  }

  return exc;
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
