# All AFL related utility functions
##########################################
# Terms
# ExprsList: a 'list' of R Expression.
# Here we only deal with these three types of R Expressions: call, name/symbol, literal
##########################################






# AFL Expressions denoted by R Expressions ------------------------------------------------------------------------





# Construct AFL expressions ---------------------------------------------------------------------------------------

#' Create AFL expressions from R expressions
#' 
#' This is a convenience function for AFL generation.
#' 
#' Any ```a | op_name(b)``` call will be translated to `op_name(a, b)` in R, then translated to AFL:
#'   `op_name(a, b)`
#'   
#' Using this syntax, we can chain multiple AFL operators
#' 
#' E.g. `'array' | filter('a > 3 and b < 4') | project('a', 'b')`
#' will be translated into: `project(filter(array, a > 3 and b < 4), 'a', 'b')`
#' Use NULL if no 2nd operand is needed. E.g. `'array' | op_count` => `op_count(array)`
#' @param ... In the ellipsis arg, any R functions right after a pipe sign `|`` is converted to 
#' a scidb operator of the same name. 
#' All regular functions are first evaluated in the calling environment, and then convereted to strings 
#' depending on the result types. 
#' ArrayOp => ArrayOp$to_afl(), v:NonEmptyVector => paste(v, collapse=','), 
#' NULL is ignored.
#' @param envir The environment where expressions are evaluated. Default: the calling env.
#' @return AFL string
#' @export
afl <- function(..., envir = parent.frame()) {
  e = rlang::expr(...)
  
  # The param 'callObj' is a R call expression
  # a | b => b(a)
  # a | b(c) => b(a, c)
  # (a|b) | m => m(b(a))
  # In all above cases, the right operand of | expression is the key of scidb operator
  convert_operator_call <- function(callObj){
    rightOperand = callObj[[3]]
    first = convert_operand(callObj[[2]]) # The 1st scidb operator param
    if(is.name(rightOperand)){
      operator = as.character(rightOperand)
      sprintf("%s(%s)", operator, first)
    }
    else if(is.call(rightOperand)){
      operator = as.character(rightOperand[[1]])
      params = c(first, sapply(as.list(rightOperand)[-1], convert_operand))
      sprintf("%s(%s)", operator, paste(params, collapse = ','))
    }
    else
      stopf("ERROR:arrayop:afl2: wrong class type of the right operand of '|' [%s]. Must be a symbol or a call. ", 
            paste(class(rightOperand), collapse = ','))
  }
  
  # The param 'obj' can be [call, primitive, ArrayOp]
  convert_operand <- function(obj) {
    if(is.call(obj)){
      func = obj[[1]]
      # if(length(func) > 1 || !is.name(func)) printf("==%s%==", as.character(func))
      if(is.name(func)){
        funcName = as.character(func)
        if(funcName == '|') # pipe operator treated specially
          return(convert_operator_call(obj))
        else if(funcName == '%as%')
          return(sprintf("%s as %s", convert_operand(obj[[2]]), convert_operand(obj[[3]])))
        else if(funcName == '(')
          return(convert_operand(obj[[2]]))
      }
    }
    evaluated = eval(obj, envir = envir)
    
    assert_inherits(evaluated, c('character', 'numeric', "integer", "integer64", "logical", "NULL", "ArrayOpBase"),
                    .symbol = "'AFL operands'")
    
    if(inherits(evaluated, 'ArrayOpBase')){
      evaluated = evaluated$to_afl()
    } else if(is.logical(evaluated)) {
      evaluated = tolower(paste(evaluated, collapse = ','))
    } else if(!is.null(evaluated)) {
      evaluated = paste(evaluated, collapse = ',')
    }
    return(evaluated)
  }

  return(convert_operand(e))
}


# AFLUtils class -----

#' AFL utility functions
#' 
#' @description 
#' Normally we don't need these functions. 
#' But in case we do, they are flexible in constructing high-order 
#' AFL expressions, e.g. AFL raw string or expressions generated in one place and
#' used in other places with or without modification.
AFLUtils <- R6::R6Class(
  "AFLUtils", cloneable = F, portable = F,
  public = list(
    #' @description 
    #' Convert filter expression(s) to AFL filter
    #' 
    #' @param e A list of R expression(s). If more than one expression is provided,
    #' they will be joined with logic AND. 
    #' @param regex_func A string of regex function implementation, default 'regex'.
    #' Due to scidb compatiblity issue with its dependencies, the regex function from boost library may not be available
    #' Currently supported options include 'rsub', and 'regex'
    #' @param ignore_case A Boolean, default TRUE. If TRUE, ignore case in string match patterns. 
    #' Otherwise, perform case-sensitive regex matches.
    #' @return An AFL filter string
    e_to_afl = function(e, regex_func = 'regex', ignore_case = TRUE) {
    
      # If 'e' is an ExprsList, then we merge it with 'AND' call by default
      # Otherwise 'e' is already a single Expression ready to be translated to AFL
      if(is.list(e)){
        e = e_merge(e)
      }
      
      operatorList <- list(AND = 'and', OR = 'or', `==` = '=', `!=` = '<>', 
                           '&&' = 'and', '&' = 'and', '||' = 'or', '|' = 'or')
    
      regexWithRsub <- function(leftOpStr, rightOpStr) {
        pattern = if(ignore_case) 's/%s//i' else 's/%s//'
        rsubExpr = sprintf("rsub(%s, '%s') = ''", leftOpStr, sprintf(pattern, rightOpStr))
        sprintf("(%s <> '' and %s)", leftOpStr, rsubExpr)
      }
    
      regexWithRegex <- function(leftOpStr, rightOpStr) {
        prefix = if(ignore_case) '(?i)' else ''
        sprintf("regex(%s, '%s%s')", leftOpStr, prefix, rightOpStr)
      }
      
      regexImplFunc = switch(
        regex_func,
        'rsub' = regexWithRsub,
        'regex' = regexWithRegex,
        stopf("ERROR:arrayop:Unknown regex function '%s'", regex_func)
      )
    
      walkThru <- function(node){
        if(is.call(node)){
          operator <- as.character(node[[1]])
          lookup <- operatorList[[operator]]
          if(!is.null(lookup)){
            operator <- lookup # Convert to AFL compliant operator if found in operatorList
          }
    
          leftOp = if(length(node) > 1L) walkThru(node[[2]]) else NULL
          # Special operators are treated specially. E.g. %like%, %in%
    
          if(operator == '%in%'){
            compared = node[[3]]  # can be single or multiple numbers/strings
            if(is.character(compared))  compared = sprintf("'%s'", compared)  # Sigle-quote strings if applicable
            subExprs = paste0(sprintf("%s = %s", leftOp, compared), collapse = ' or ')
            return(sprintf("(%s)", subExprs))
          }
    
          if(operator == '%like%' || operator %in% c('%contains%', '%starts_with%', '%ends_with%')){
            compared = node[[3]]  # can be single or multiple strings
            assert_single_str(compared, "ERROR:arrayop: right operand of %%like%% function must be a single string.")
            if(operator != '%like%'){  # escape special chars if not directly using regex pattern
              escaped = compared
              escaped = gsub("(\\\\)", "\\\\\\\\\\1", escaped)
              escaped = gsub("([][*()'])", "\\\\\\1", escaped) # \\1 is for the original char
            }
            rightOp = switch(
              operator,
              '%contains%' = sprintf(".*%s.*", escaped),
              '%starts_with%' = sprintf("%s.*", escaped),
              '%ends_with%' = sprintf(".*%s", escaped),
              compared
            )
            return(regexImplFunc(leftOp, rightOp))
          }
    
          if(operator == '%not_in%'){
            compared = node[[3]]
            if(is.character(compared))  compared = sprintf("'%s'", compared)  # Sigle-quote strings if applicable
            subExprs = paste0(sprintf("%s <> %s", leftOp, compared), collapse = ' and ')
            return(sprintf("(%s)", subExprs))
          }
          
          if(operator == 'is_null'){
            return(sprintf("%s is null", leftOp))
          }
          
          if(operator == 'not_null'){
            return(sprintf("%s is not null", leftOp))
          }
          
          if(operator == '('){
            return(sprintf("(%s)", walkThru(node[[2]])))
          }
          
          if(operator == '!'){
            return(sprintf("not %s", walkThru(node[[2]])))
          }
          
          if(operator == 'count' && is.null(leftOp)){ 
            # special case for aggregation function becasue * is not a valid R operand
            return("count(*)")
          }
          
          # Replace operator name from R convetion to SciDB operators/functions
          operator = switch (operator,
            "nchar" = "strlen",
            "if" = "iif",
            operator
          )
    
          # Regular operators are treated recursively
          operands <- sapply(node[-1], walkThru)
    
          if(!is.element(operator, c('and', 'or')) && grepl('\\w+', operator)) {
            # Regular alphanumerical operator, e.g. strlen
            return(sprintf("%s(%s)", operator, paste(operands, collapse = ',')))
          }
          # Speical operator, e.g. >=, <=, != NOTE: 'and', 'or' are treated as comparison operators.
          return(paste(operands, collapse = sprintf(' %s ', operator)))
        } else if (is.name(node)) {
          s = as.character(node)
          return(s)
        } else if (is.atomic(node)) {
          if(is.numeric(node))
            return(as.character(node))
          else if(is.logical(node))
            return(tolower(as.character(node)))
          else if(is.character(node))
            # Escape single quotes in strings
            return(paste0("'", gsub("(')", "\\\\\\1", node), "'"))
        } else {
          stop(sprintf('Unknow class: [%s]', paste(class(node), collapse = ',')))
        }
      }
    
      walkThru(e)
    }
    ,
    #' @description 
    #' Convert API ... args to an R expression vector (deprecated)
    #' 
    #' Some API functions include ... arg to represent arbitrary search criteria. 
    #' This provides flexibility and simplifies API function signatures, but only supports limited advanced search,
    #' e.g. xxx_contains, xxx_range, xxx_not.
    #' 
    #' 
    #' Eg. name_contains = 'str' => name %contains% 'str'
    #' Eg. value_range = c(1, 9) => c(value >= 1, value <= 9)
    #' @param ... API ellipsis args
    #' @param .dots Explicitly provide a parameter list. If not NULL, the ellipsis params are ignored
    #' @return A list of AFL expressions in R
    args_to_expressions = function(..., .dots = NULL) {
      rangeExpr <- function(name, value) {
        if (is.numeric(value)) {
          # 1st lower bound, 2nd upper bound, either can be missing (NA/NULL)
          if(!is.na(value[[1]]) && !is.na(value[[2]])) {
            assert(value[[1]] <= value[[2]], "ERROR: Illegal range values for param '%s': [%s,%s]. Min must <= max.", 
                   as.character(name), value[[1]], value[[2]])
            return(e(AND(!!name >= !!value[[1]], !!name <= !!value[[2]])))
          }
          if(!is.na(value[[1]]))  return(e(!!name >= !!value[[1]]))
          if(!is.na(value[[2]]))  return(e(!!name <= !!value[[2]]))
        }
        stop(sprintf("Range values for param '%s' must be a one-number or two-number vector, but got '%s' indstead.
                     Eg. c(5, 99)", as.character(name), toString(value)))
      }
    
      containsExpr <- function(name, value) {
        if (is.character(value) && length(value) >= 1) {
          quoted <- sprintf(".*%s.*", value)
          return(e(!!name %like% !!quoted))
        }
        stopf("Right hand side of a contains expression must be a R character (length >= 1). Got: %s", value)
      }
    
      notExpr <- function(name, value) {
        if(length(value) == 0) return(e(is_not_null(!!name)))
        if(length(value) == 1){
          if(is.na(value)) return(e(is_not_null(!!name)))
          return(e(!!name != !!value))
        }
        else if(length(value) > 1)
          return(e(!!name %not_in% !!value))
        stopf("Right hand side of a Not expression must be a non-empty value, but got: %s", paste(class(value), collapse = ','))
      }
    
      convert <- function(nameExpr, value) {
        indexing <- sapply(suffixes, grepl, nameExpr)
        matchedSuffix <- suffixes[indexing]
        conversionFunc <- suffixFuncs[indexing]
        if (length(matchedSuffix) == 1) { # If this is a special name which has a 'contains' or 'range' suffix
          fieldName <- gsub(matchedSuffix, "", nameExpr) # strip out the suffix to get field name
          return(conversionFunc[[1]](as.name(fieldName), value))
        } 
        else { # Regular equality tests
          quotedName <- as.name(nameExpr)
          if(length(value) > 1)
            return(e(!!quotedName %in% !!value))
          if(is.na(value) || is.null(value))
            return(e(is_null(!!quotedName)))
          return(e(!!quotedName == !!value)) # Just a regular equal expression
        }
      }
      optionalArgs = if(is.null(.dots)) list(...) else {
        assert(is.list(.dots), "ERROR:args_to_expressions:.dots if provided must be a named list")
        .dots
      }
      # optionalArgs <- list(...)
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
    ,
    #' @description 
    #' Create a list of R expressions
    #' 
    #' The ... ellipsis arg can include arbitrary expressions, where all names are preserved in their literal forms,
    #' **except** for those prefixed with !! (double exclamation marks) which will be evaluated to their actual values 
    #' in the calling environment.
    #' 
    #' Besides common comparison operators including `==`, `>`, `<`, `>=`, `<=`, `!=`, there are a few special operators
    #' supported to ease AFL generation:
    #'   - `%in%` semantically similar to R. `a %in% !!c(1,2,3)` will be translated to `(a == 1 or a == 2 or a == 3)`
    #'   - `%like%` for string regex matching. 
    #' @param ... The ellipsis arg can have multiple items as expressions, but NO named items as in a named list.
    #' @return A list of R expressions
    e = function(...) {
      allExprs <- rlang::exprs(...)
      # No named args allowed here to avoid confusion.
      # E.g. e(a = 3) will throw an error, use e(a == 3) instead.
      argNames = names(allExprs)
      namedArgs <- argNames[argNames != '']
      assert_not_has_len(namedArgs, "Please use == for equality tests with args: %s", paste(namedArgs, collapse = ', '))
      return(structure(allExprs, names = NULL))
    }
    ,
    #' @description 
    #' Merge multiple R expressions into one
    #' 
    #' Merge an ExprsList into a single Expression so that it can be used as a FilterExpr
    #' @param el A list of R expressions
    #' @param mode 'AND' | 'OR'. Logical relationships when merging the expressions.
    #' @return R expression
    e_merge = function(el, mode = 'AND'){
      if(length(el) == 0) return(NULL)
      if(length(el) == 1) return(el[[1]])  # If only one expression, then no need to embed it in 'AND'/'OR'
    
      if(mode == 'AND' || mode == 'OR'){
        concatList <- c(list(as.name(mode)), el)
        return(as.call(concatList))
      }
      stop(sprintf(".el_merge error: mode must be 'AND' or 'OR', but got %s", mode))
    }
    ,
    #' @description 
    #' Just multple fields with sep = ','
    #' 
    #' Default behavior: `paste(..., sep = sep, collapse = sep)` where `sep = ','`
    #' 
    #' afl(...) will convert vectors to joined strings separated by `,`. 
    #' This function is useful in concatenating multiple vectors in parallel, 
    #' e.g. joining a new field vector and expression vector for the `apply` operator.
    #' @param ... Multiple string vectors
    #' @param sep A single character string, defaullt ",", as field separator.
    join_fields = function(..., sep = ',') {
      paste(..., sep = sep, collapse = sep)
    }
    ,
    #' @description 
    #' Validate a filter expression
    #'
    #' Current only report errors on:
    #'   1. Name symbols that are not known schema fields, defined by 
    #'   param `all_fields`
    #'   2. Non-atomic R 'values' in the expression
    #' @param filter_expr AFL captured as a single R expression
    #' @param all_fields A list of strings as the scope of valid fields
    #' @return A list object with named elements:
    #'  success:bool, absent_fields: c(''), error_msgs: c('')
    validate_filter_expr = function(filter_expr, all_fields) {
      absentFields = c()
      errorMsgs = c()
    
      # a recurrsive function that traverses every element in filter_expr
      traverseSingleExpr <- function(rExpr) {
        if (is.name(rExpr)) {
          symbolName <- as.character(rExpr)
          if (!is.element(symbolName, all_fields)) {
            assign('absentFields', c(absentFields, symbolName), inherits = TRUE)
          }
        } else if (is.call(rExpr)) {
          # rExpr is a call, then traverse its args
          lapply(rExpr[-1], traverseSingleExpr)
        } else {
          # Neither a name symbol nor a call node, then must be an atomic vector, e.g. 42, c(3, 4), 'abc'
          if (!is.atomic(rExpr)) {
            assign('errorMsgs',
              c(errorMsgs, sprintf("Non-atomic '%s' object can not be used in filter expression", class(rExpr))),
              inherits = TRUE)
          }
        }
      }
    
      traverseSingleExpr(filter_expr)
    
      return(list(
        success = .is_empty(absentFields) && .is_empty(errorMsgs),
        absent_fields = absentFields, error_msgs = errorMsgs
      ))
    }
  )
)

aflutils = AFLUtils$new()

#' @export aflutils
NULL
