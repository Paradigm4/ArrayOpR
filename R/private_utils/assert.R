# Make an assertion. Throw an error if it fails.
# 
# When an error is thrown, the call traceback is also printed to help debug. 
# 
# param cond: An R expression that evaluates to a boolean value. FALSE indicates assertion failure.
# param error_fmt: A string template where {.symbol} is replaced by the literal `cond` expression.
#  the actual expression is determined by the calling env, which defaults to the parent.frame()  
# param .nframe: the number of nested function frames. 0 means the env where this function is called from
# param .symbol: the R expression of `cond` in string form
assertf = function(cond, 
                   error_fmt = "'{.symbol}' is not true", 
                   .nframe = 0, 
                   .symbol = deparse(substitute(cond)),
                   ...
                   ) {
  # None of the params is evaluated if `cond` is TRUE
  if(!cond) {
    template = glue("{error_fmt}\n{{trace_call}\n.")
    trace_call = deparse(sys.call(-.nframe - 1))
    var_env = list(error_fmt = error_fmt, trace_call = trace_call, ...)
    stop(glue(template, .envir = list(error_fmt = error_fmt, )), call. = FALSE)
  }
}

assert_pos_num = function(num, error_fmt = "'{.symbol}' must be > 0, but got: {.value}", .nframe = 0, .symbol = deparse(substitute(num))) {
  assertf(num > 0, error_fmt = glue(error_fmt), .nframe = .nframe + 1, .symbol = .symbol, .value = num)
}

assert_null = function(value, error_fmt = "'{.symbol}' must be NULL", .nframe = 0, .symbol = deparse(substitute(value))) {
  assertf(is.null(value), error_fmt = glue(error_fmt), .nframe = .nframe + 1, .symbol = .symbol, .value = value)
}

assert_inherits = function(value, expected_class_names,
                           error_fmt = "{.symbol} is should inherit from one of the class(es): [{.expected_class_names}], but got class of: {.actual_class_names}",
                           .nframe = 0, .symbol = deparse(substitute(value))
){
  assertf(inherits(value, class_names), 
          error_fmt = glue(error_fmt),
          .nframe = .nframe + 1,
          .symbol = .symbol,
          .expected_class_names = paste(expected_class_names, collapse = ','),
          .actual_class_names = paste(class(value), collapse = ',')
          )
}

assert_single_str = function(
  value, 
  error_fmt = "{.symbol} should be a string, ie. length-1 character, but got length-{.length} {.class_names}", 
  .nframe = 0,
  .symbol = deparse(substitute(value))
) {
  assertf(
    length(value) == 1 && is.character(value),
    error_fmt = error_fmt,
    .nframe = .nframe + 1,
    .symbol = .symbol,
    .length = length(value),
    .class_names = paste(class(value), collapse = ',')
  )
}

assert_single_num = function(
  value, 
  error_fmt = "{.symbol} should be a number, ie. length-1 numeric vector, but got length-{.length} {.class_names}", 
  .nframe = 0,
  .symbol = deparse(substitute(value))
) {
  assertf(
    length(value) == 1 && is.numeric(value),
    error_fmt = error_fmt,
    .nframe = .nframe + 1,
    .symbol = .symbol,
    .length = length(value),
    .class_names = paste(class(value), collapse = ',')
  )
}
