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

.ifelse <- function(condition, yes, no) if(condition) yes else no
.has_len <- function(...) rlang::has_length(...)

.random_attr_name <- function(n = 4){
  sprintf("_%s", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
}


`%u%` = function(lhs, rhs) base::union(lhs, rhs)
`%-%` = function(lhs, rhs) base::setdiff(lhs, rhs)
`%n%` = function(lhs, rhs) base::intersect(lhs, rhs)


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
