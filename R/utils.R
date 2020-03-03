# System and validation -------------------------------------------------------------------------------------------

# Here we use R's naming convention.
# fmt = format, msg = message

stopf <- function(fmt, ...) { stop(sprintf(fmt, ...)) }

printf <- function(fmt, ...) { print(sprintf(fmt, ...)) }

assert <- function(cond, errorMsgFmt = '', ...) { if(!cond) stop(sprintf(errorMsgFmt, ...))}

assert_has_len <- function(obj, ...) { assert(rlang::has_length(obj), ...) }
assert_not_has_len <- function(obj, ...) { assert(!rlang::has_length(obj), ...) }
assert_named_list <- function(obj, ...) {
  assert(.has_len(names(obj)) && all(names(obj) != ''), ...)
}

assert_single_str <- function(obj, ...) {
  assert(is.character(obj) && length(obj) == 1, ...)
}

assert_single_number <- function(obj, ...) {
  assert(is.numeric(obj) && length(obj) == 1, ...)
}

# %s in errorMsgFmt will be replaced the concatenated fields; only one %s is allowed. 
# Extra format placeholders should be denoted by %%s, %%d, etc. due to the extra layer of indirection.
assert_no_fields <- function(fields, errorMsgFmt = "Field(s) not empty: %s", ..., sep = ','){
  assert_not_has_len(fields, sprintf(errorMsgFmt, paste(fields, collapse = sep)), ...)
}

.ifelse <- function(condition, yes, no) if(condition) yes else no
.has_len <- function(...) rlang::has_length(...)

.random_attr_name <- function(n = 4){
  sprintf("_%s", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
}


`%u%` = function(lhs, rhs) base::union(lhs, rhs)
`%-%` = function(lhs, rhs) base::setdiff(lhs, rhs)
`%n%` = function(lhs, rhs) base::intersect(lhs, rhs)


#' Invert names and values of a list (or named vector)
#'
#' @param obj a named list
#' @param .as.list Return a list if set TRUE; otherwise a named vector
#' @return A list or named vector whose key/values are inverted from the original `obj`
invert.list = function(obj, .as.list = T) {
  res = structure(
    unlist(mapply(rep, names(obj), sapply(obj, length)), use.names=F),
    names = unlist(obj)
  )
  if(.as.list) res = as.list(res)
  res
}

log_job = function(job, msg = '', done_msg = 'done') {
  cat(sprintf("%s ... ", msg))
  result = force(job)
  cat(sprintf("%s. [%s]\n", done_msg, Sys.time()))
  invisible(result)
}

# Output job duration.
# 
# Fields of durations are `names(proc.time())`, i.e. [user.self, sys.self, elapsed, user.child, sys.child]
# Units are seconds.
# @param job An R expression to be evaluated and returned
# @param msg A message shown as the log entry, followed by a timestamp and time durations.
# @return evaluated 'job' 
log_job_duration = function(job, msg = '', done_msg = 'done') {
  cat(sprintf("%s ... ", msg))
  start = proc.time()
  result = force(job)
  duration = proc.time() - start
  cat(sprintf("%s. \t[%s]\t%s\n", done_msg, Sys.time(), paste(duration, collapse = '\t')))
  invisible(result)
}

.to_signed_integer_str = function(values) {
  single_value = function(v) {
    if(v == 0) '' else sprintf("%+d", v)
  }
  values = as.integer(values)
  vapply(values, single_value, FUN.VALUE = '')
}

# param named_values: A named list or named vector
# return The names of the elements. If an element has no name, then its string representation will be used as name.
.get_element_names = function(named_values) {
  if (!.has_len(names(named_values)))
    as.character(named_values)
  else {
    mapply(function(name, value) {
      if (name == '') value
      else name
    }, names(named_values), named_values, USE.NAMES = F)
  }
}
