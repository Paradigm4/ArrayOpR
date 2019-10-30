# Given a list of Fields, convert to a named list whose names are Field names.
.makeNamedFieldList <- function(fields) {
  # Ensure every field is inherited from Field class
  allInheritField <- all(sapply(fields, inherits, "Field"))
  stopifnot(allInheritField)
  fieldNames <- sapply(fields, function(x) x$name)
  return(structure(fields, names = fieldNames))
}


# System and validation -------------------------------------------------------------------------------------------

# Here we use R's naming convention.
# fmt = format, msg = message

stopf <- function(fmt, ...) { stop(sprintf(fmt, ...)) }

printf <- function(fmt, ...) { print(sprintf(fmt, ...)) }

assert <- function(cond, errorMsgFmt, ...) { if(!cond) stop(sprintf(errorMsgFmt, ...))}

assert_has_len <- function(obj, ...) { assert(rlang::has_length(obj), ...) }
assert_not_has_len <- function(obj, ...) { assert(!rlang::has_length(obj), ...) }


.ifelse <- function(condition, yes, no) if(condition) yes else no
.has_len <- function(...) rlang::has_length(...)

.random_attr_name <- function(n = 4){
  sprintf("_%s", rawToChar(as.raw(sample(c(65:90,97:122), n, replace=TRUE))))
}
