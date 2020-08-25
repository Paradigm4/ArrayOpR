# System and validation -------------------------------------------------------------------------------------------

# lhs: left-hand side
# rhs: right-hand side

`%u%` = function(lhs, rhs) base::union(lhs, rhs)
`%-%` = function(lhs, rhs) base::setdiff(lhs, rhs)
`%n%` = function(lhs, rhs) base::intersect(lhs, rhs)
# `%||%` = function(lhs, rhs) if(is.null(lhs)) rhs else lhs
`%?%` = function(lhs, rhs) if(length(lhs) == 0L) rhs else lhs


# Here we use R's naming convention.
# fmt = format, msg = message

stopf <- function(fmt, ...) { stop(sprintf(fmt, ...)) }

printf <- function(fmt, ...) { print(sprintf(fmt, ...)) }
catf <- function(fmt, ...) { cat(sprintf(fmt, ...)) }

assert <- function(cond, errorMsgFmt = '', ...) { if(!cond) stop(sprintf(errorMsgFmt, ...))}

assert_has_len <- function(obj, ...) { assert(rlang::has_length(obj), ...) }
assert_not_has_len <- function(obj, ...) { assert(!rlang::has_length(obj), ...) }
assert_named_list <- function(obj, ...) {
  assert(.not_empty(names(obj)) && all(names(obj) != ''), ...)
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
.not_empty <- function(x) length(x) > 0L
.is_empty <- function(x) length(x) == 0L

.random_attr_name <- function(n = 4){
  sprintf("%s_", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
}

.random_field_name = .random_attr_name


.random_array_name <- function(prefix = "Rarrayop", n = 10){
  sprintf("%s_%s_", prefix, rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
}



#' Invert names and values of a list (or named vector)
#'
#' @param obj a named list
#' @param .as.list Return a list if set TRUE; otherwise a named vector
#' @return A list or named vector whose key/values are inverted from the original `obj`
invert.list = function(obj, .as.list = T) {
  if(.is_empty(obj)) return(list())
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

print_error <- function(...){ 
  cat(sprintf(...), file=stderr())
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
  if (.is_empty(names(named_values)))
    as.character(named_values)
  else {
    mapply(function(name, value) {
      if (name == '') value
      else name
    }, names(named_values), named_values, USE.NAMES = F)
  }
}

# Remove all NULL entrieds from a list. Same as plyr::compact
.remove_null_values = function(list_values) {
  Filter(Negate(is.null), list_values)
}

new_named_list = function(values, names){
  if(.is_empty(values)) return(list())
  as.list(structure(values, names = as.character(names)))
}

# Make an env ----
# Make an env with function/objs and a hidden parent env.
#
# Use the returned env to simulate a singleton with static methods
make_env = function(
  public = list(), 
  private = list(),
  root_env = parent.frame(),
  add_self_ref = TRUE, 
  add_private_ref = TRUE, 
  lock_env = FALSE, 
  lock_binding = FALSE
){
  return_list = public
  
  move_funcs_to_env = function(e, target_env = e) {
    is_name_func = function(name) is.function(e[[name]])
    funcNames = Filter(is_name_func, ls(e, all.names = TRUE))
    sapply(funcNames, function(x) {
      environment(e[[x]]) <- target_env
    })
  }
  
  parentEnv = if(is.list(private)) {
    list2env(private, parent = root_env)
  } else {
    stop("ERROR: make_env: param 'private' must be a named list, but got: [%s]", paste(class(private), collapse = ","))
  }
  result = if(is.list(public))  {
    list2env(public, parent = parentEnv)
  } else if(is.environment(public)) {
    parent.env(public) <- parentEnv
    public
  } else { stop("ERROR: make_env: param 'public' must be a named list or R env") }
  
  move_funcs_to_env(result)
  move_funcs_to_env(parentEnv, result)
  
  if(add_self_ref){
    result[[".self"]] <- result
    parentEnv[['.self']] <- result
  }
  if(add_private_ref){
    result[[".private"]] <- parentEnv
    parentEnv[[".private"]] <- parentEnv
  }
  if(lock_env || lock_binding){
    # we cannot add new variables to a locked env
    # we cannot change variable bindings in a lockBinding env
    # variable bindings can be individually locked, but we treated them consistently
    if(lock_env) {
      lockEnvironment(result, lock_binding)
    } else {
      lockBinding(ls(result), env = result)
    }
  }
  result
}
