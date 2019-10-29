# All AFL related utility functions
##########################################
# Terms
# ExprsList: a 'list' of R Expression.
# Here we only deal with these three types of R Expressions: call, name/symbol, literal
##########################################

#' @export
afl_filter_from_expr <- function(e) {

  # If 'e' is an ExprsList, then we merge it with 'AND' call by default
  # Otherwise 'e' is already a single Expression ready to be translated to AFL
  if(is.list(e)){
    e = e_merge(e)
  }

  operatorList <- list(AND = 'and', OR = 'or', `==` = '=', `!=` = '<>')

  walkThru <- function(node){
    if(is.call(node)){
      operator <- as.character(node[[1]])
      lookup <- operatorList[[operator]]
      if(!is.null(lookup)){
        operator <- lookup # Convert to AFL compliant operator if found in operatorList
      }


      fieldName = as.character(node[[2]])
      # Special operators are treated specially. E.g. %like%, %in%

      if(operator == '%in%'){
        compared = node[[3]]  # can be single or multiple numbers/strings
        if(is.character(compared))  compared = sprintf("'%s'", compared)  # Sigle-quote strings if applicable
        subExprs = paste0(sprintf("%s = %s", fieldName, compared), collapse = ' or ')
        return(sprintf("(%s)", subExprs))
      }

      if(operator == '%like%'){
        compared = node[[3]]  # can be single or multiple strings
        rsubExpr = paste0(sprintf("rsub(%s, 's/%s//i') = ''", fieldName, compared), collapse = ' or ')
        return(sprintf("(%s <> '' and (%s))", fieldName, rsubExpr))
      }

      if(operator == '%not_in%'){
        compared = node[[3]]
        if(is.character(compared))  compared = sprintf("'%s'", compared)  # Sigle-quote strings if applicable
        subExprs = paste0(sprintf("%s <> %s", fieldName, compared), collapse = ' and ')
        return(sprintf("(%s)", subExprs))
      }

      # Regular operators are treated recursively
      operands <- sapply(node[-1], walkThru)

      if(!is.element(operator, c('and', 'or')) && grepl('\\w+', operator)) {
        # Regular alphanumerical operator, e.g. strlen
        return(sprintf("%s(%s)", operator, paste(operands, collapse = ',')))
      }
      # Speical operator, e.g. >=, <=, != NOTE: 'and', 'or' are treated as comparison operators.
      return(paste(operands, collapse = sprintf(' %s ', operator)))
    } else if (is.name(node)) {
      return(as.character(node))
    } else if (is.atomic(node)) {
      if(is.numeric(node))
        return(as.character(node))
      else
        return(sprintf("'%s'", node))
    } else {
      stop(sprintf('Unknow class: %s', print(node)))
    }
  }

  walkThru(e)
}

# Convert search criteria in function argument format to a list of expressions
# Eg. name_contains = 'str' => name %contains% 'str'
# Eg. value_range = c(1, 9) => c(value >= 1, value <= 9)
.args_to_expressions <- function(...) {
  rangeExpr <- function(name, value) {
    if (is.numeric(value)) {
      concateExprs <- c()
      # 1st lower bound, 2nd upper bound, either can be missing (NA/NULL)
      if(!is.na(value[[1]]))  concateExprs <- c(concateExprs, e(!!name >= !!value[[1]]))
      if(!is.na(value[[2]]))  concateExprs <- c(concateExprs, e(!!name <= !!value[[2]]))
      return(concateExprs)
    }
    stop(sprintf("Range values for field '%s' must be a two-number vector, but got '%s' indstead.
                 Eg. c(5, 99)", refField, toString(value)))
  }

  containsExpr <- function(name, value) {
    if (is.character(value) && length(value) >= 1) {
      quoted <- sprintf(".*%s.*", value)
      return(e(!!name %like% !!quoted))
    }
    stopf("Right hand side of a contains expression must be a R character (length >= 1). Got: %s", value)
  }

  notExpr <- function(name, value) {
    if(length(value) == 1)
      return(e(!!name != !!value))
    else if(length(value) > 1)
      return(e(!!name %not_in% !!value))
    stopf("Right hand side of a Not expression must be a non-empty value, but got: %s", value)
  }

  convert <- function(nameExpr, value) {
    indexing <- sapply(suffixes, grepl, nameExpr)
    matchedSuffix <- suffixes[indexing]
    conversionFunc <- suffixFuncs[indexing]
    if (length(matchedSuffix) == 1) { # If this is a special name which has a 'contains' or 'range' suffix
      fieldName <- gsub(matchedSuffix, "", nameExpr) # strip out the suffix to get field name
      return(conversionFunc[[1]](as.name(fieldName), value))
    } else {
      quotedName <- as.name(nameExpr)
      if(length(value) > 1)
        return(e(!!quotedName %in% !!value))
      return(e(!!quotedName == !!value)) # Just a regular equal expression
    }
  }

  optionalArgs <- list(...)
  argNames <- names(optionalArgs)
  if (length(optionalArgs) == 0) {
    return(list())
  }

  if (is.null(argNames) || is.element("", argNames)) {
    stop(sprintf("Every argument used as a search criterion must have a name.
Make sure you passed in a named list: %s", str(optionalArgs)))
  }

  suffixes <- c("_range$", "_contains$", "_not$") # Add $ to ensure match at right end
  suffixFuncs <- c(rangeExpr, containsExpr, notExpr)

  res <- as.list(mapply(convert, argNames, optionalArgs))
  names(res) <- NULL
  res <- do.call(c, res, quote = TRUE)
  res
}


# AFL Expressions denoted by R Expressions ------------------------------------------------------------------------

# Merge an ExprsList into a single Expression so that we can create hiararchical expressions,
# which is useful in complex queries.
e_merge <- function(el, mode = 'AND'){
  if(length(el) == 0) return(NULL)
  if(length(el) == 1) return(el[[1]])  # If only one expression, then no need to embed it in 'AND'/'OR'

  if(mode == 'AND' || mode == 'OR'){
    concatList <- c(list(as.name(mode)), el)
    return(as.call(concatList))
  }
  stop(sprintf(".el_merge error: mode must be 'AND' or 'OR', but got %s", mode))
}

# Return a list of FilterExpr from ..., only the unmaed args will be used
# The ... args have to be literal, ie. no variable substitution occurs,
#   Unless an variable is prefixed with !! (e.g. !!v), in which case !!v will be subsituted with eval(v)
e <- function(...) {
  allExprs <- rlang::exprs(...)
  # No named args allowed here to avoid confusion.
  # E.g. e(a = 3) will throw an error, use e(a == 3) instead.
  argNames = names(allExprs)
  namedArgs <- argNames[argNames != '']
  assert_not_has_len(namedArgs, "Please use == for equality tests with args: %s", paste(namedArgs, collapse = ', '))
  return(structure(allExprs, names = NULL))
}


# Construct AFL expressions ---------------------------------------------------------------------------------------

# Create AFL expressions from R expressions
# Any "a %op_name% b" call will be translated to %op_name%(a, b) in R, then translated to AFL:
#   op_name(a, b)
# Using this syntax, we can chain multiple AFL operators
# E.g. 'array' %filter% 'a > 3 and b < 4' %project% .afl_join_fields('a', 'b')
# will be translated into: project(filter(array, a > 3 and b < 4), 'a', 'b')
# Use NULL if no 2nd operand is needed. E.g. 'array' %op_count% NULL => op_count(array)
.afl <- function(...) {
  e = rlang::expr(...)
  envir = parent.frame()

  convert_call <- function(callObj){
    func = callObj[[1]]
    # func can be another function call or just any regular user functions (ie. no %)
    if(!is.name(func) || substr(as.character(func), 1, 1) != '%')
      return(eval(callObj, envir = envir))

    # Here 'func' is a Scidb operator
    rawName = as.character(func)
    callName = gsub('%', '', rawName)
    operatorArgs = plyr::compact(sapply(callObj[-1], convert_operand))
    sprintf("%s(%s)", callName, paste(operatorArgs, collapse = ','))
  }

  convert_operand <- function(obj) {
    if(is.call(obj))
      return(convert_call(obj))
    return(eval(obj, envir = envir))
  }

  res = convert_operand(e)
  return(res)
}

.afl_join_fields <- function(..., sep = ',') {
  paste(..., sep = sep, collapse = sep)
}
