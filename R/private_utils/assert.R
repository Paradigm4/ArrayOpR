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
                   error_fmt = "Failed assertion: '{.symbol}'.", 
                   .nframe = 0, 
                   .symbol = deparse(substitute(cond)),
                   ...
                   ) {
  # None of the params is evaluated if `cond` is TRUE
  if(!cond) {
    template = sprintf("%s\n{.stack_trace}\n", error_fmt)
    var_env = list(
      .symbol = .symbol, 
      .stack_trace = deparse(sys.call(-.nframe - 1)), 
      ...
    )
    stop(glue(template, .envir = var_env), call. = FALSE)
  }
}

assert_empty = function(list_or_vec, 
                        error_fmt = "'{.symbol}' should be empty (0-length), but got length-{.length}: [{.value}]", 
                        .nframe = 0, .symbol = deparse(substitute(list_or_vec))) {
  assertf(length(list_or_vec) == 0L, error_fmt = error_fmt, 
          .nframe = .nframe + 1, .symbol = .symbol, 
          .value = paste(list_or_vec, collapse = ','),
          .length = length(list_or_vec)
          )
}

assert_not_empty = function(list_or_vec, 
                        error_fmt = "'{.symbol}' should not be empty (0-length)", 
                        .nframe = 0, .symbol = deparse(substitute(list_or_vec))) {
  assertf(length(list_or_vec) > 0L,
          error_fmt = error_fmt, 
          .nframe = .nframe + 1, .symbol = .symbol
          )
}

assert_unique_named_list <- function(obj, 
                              error_fmt = "'{.symbol}' should be a named list where each element has a unique name",
                              .nframe = 0,
                              .symbol = deparse(substitute(obj))) {
  assertf(is.list(obj) && 
          length(names(obj)) > 0L && 
          all(names(obj) != '') && !anyDuplicated(names(obj)), 
          error_fmt = error_fmt, 
          .nframe = .nframe + 1, .symbol = .symbol
  )
}

assert_pos_num = function(num, error_fmt = "'{.symbol}' must be > 0, but got: {.value}", .nframe = 0, .symbol = deparse(substitute(num))) {
  assertf(num > 0, error_fmt = error_fmt, .nframe = .nframe + 1, .symbol = .symbol, .value = num)
}

assert_null = function(value, error_fmt = "'{.symbol}' must be NULL", .nframe = 0, .symbol = deparse(substitute(value))) {
  assertf(is.null(value), error_fmt = error_fmt, .nframe = .nframe + 1, .symbol = .symbol, .value = value)
}

assert_inherits = function(value, expected_class_names,
                           error_fmt = "{.symbol} should inherit from one of the class(es): [{.expected_class_names}], but got class of: [{.actual_class_names}]",
                           .nframe = 0, .symbol = deparse(substitute(value))
){
  assertf(inherits(value, expected_class_names), 
          error_fmt = error_fmt,
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

assert_single_bool = function(
  value, 
  error_fmt = "{.symbol} should be a boolean, ie. length-1 logical vector, but got length-{.length} {.class_names}", 
  .nframe = 0,
  .symbol = deparse(substitute(value))
) {
  assertf(
    length(value) == 1 && is.logical(value),
    error_fmt = error_fmt,
    .nframe = .nframe + 1,
    .symbol = .symbol,
    .length = length(value),
    .class_names = paste(class(value), collapse = ',')
  )
}
