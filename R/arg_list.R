#' ArgList
#' 
#' Encapsulate arg list of function calls
#' 
#' Encapsulate arg list of function calls to avoid length argument passing among R functions
#' @export 
ArgList <- R6::R6Class('ArgList',
  # explicit_args: a named list that stores explicitly passed-in args
  # effective_args: a named list that stores effective args in caller's function
  # including those missing but with a default value
  active = NULL,
  portable = FALSE, # sacrifice portability for simplicity (ie. no self/private object references)
  
  private = list(
    .choose_list = function(explicit) if(explicit) explicit_args else effective_args
  )
  ,
  
  public = list(
    
    explicit_args = NULL, 
    effective_args = NULL,
    
    initialize = function(explicit_args, effective_args){
      self$explicit_args = explicit_args
      self$effective_args = effective_args
    }
    ,
    # All functions with an explicit arg, if set true, only checking on explicitly pass-in args;
    # If set false, checking on all effective args in caller's function scope.

    get_len = function(explicit = FALSE) {
      length(.choose_list(explicit))
    },

    # Return a logical vector which has the same length with 'argNames', each indicating whether an arg is present
    has_args = function(argNames, explicit = FALSE){
        hasName(.choose_list(explicit), argNames)
    },

    # Return if all the args specified in 'argNames' are present
    has_all_args = function(..., explicit = FALSE){
      all(has_args(c(...), explicit = explicit))
    },

    # Return if all the args specified in 'argNames' are present
    has_any_args = function(..., explicit = FALSE){
      any(has_args(c(...), explicit = explicit))
    },

    # Return if there are 'count' argNames' if the ArgList
    has_args_count = function(argNames, count, explicit = FALSE) {
      sum(has_args(argNames, explicit)) == count
    },

    has_scalar_args = function(..., explicit = FALSE) {
      argNames = c(...)
      has_all_args(argNames, explicit = explicit) && all(lapply(self[argNames], length) == 1)
    },

    has_multi_len_args = function(argNames, explicit = FALSE) {
      has_all_args(argNames, explicit = explicit) && all(lapply(self[argNames], length) > 1)
    },

    # Return how many args in 'argNames' are present in the ArgList
    args_count = function(argNames, count, explicit = FALSE) {
      sum(has_args(argNames, explicit))
    },

    # Create a new ArgList, a subset of the current ArgList. Useful for multiple args sets in one api call.
    subset = function(argNames, inverse = FALSE) {
       mask1 = if(!inverse) names(explicit_args) %in% argNames else ! names(explicit_args) %in% argNames
       mask2 = if(!inverse) names(effective_args) %in% argNames else ! names(effective_args) %in% argNames
       return(ArgList$new(explicit_args = explicit_args[mask1], effective_args = effective_args[mask2]))
    },

    # Return a new ArgList with renamed fields. Triple-dot args should an even number of strings, consist of
    # oldName1, newName1[, oldName2, newName2] [...]
    rename = function(...){
      pairs = c(...)
      len = length(pairs) / 2
      assert(is.character(pairs) && len > 0 && length(pairs) %% 2 == 0,
             "ArgList rename must take an even number of string args, but got: %s", pairs)
      originals = pairs[1 + 2 * 0:(len-1)]
      replaced = pairs[2 + 2 * 0:(len-1)]

      newNames = names(explicit_args)
      newNames[na.omit(match(originals, newNames))] <- replaced
      newExplicitArgs = structure(explicit_args, names = newNames)

      newNames = names(effective_args)
      newNames[na.omit(match(originals, newNames))] <- replaced
      newEffectiveArgs = structure(effective_args, names = newNames)
      return(ArgList$new(effective_args = newEffectiveArgs, explicit_args = newExplicitArgs))
    },

    assert_exclusive_args = function(argNames, errorTemplate = "Argument '%s' cannot be specified with '%s'"){
      marks = has_args(argNames)
      indicesOfExistingArgs = which(marks)
      if(length(indicesOfExistingArgs) >= 2){
        stopf(errorTemplate, argNames[[indicesOfExistingArgs[[1]]]], argNames[[indicesOfExistingArgs[[2]]]])
      }
    },

    to_exprs_list = function(explicit = FALSE) {
      do.call(args_to_expressions, .choose_list(explicit))
    }
    ,
    to_list = function() {
      effective_args
    }
  )
)

#' @export
`[[.ArgList` <- function(x, argName) {
  x$effective_args[[argName]]
}

#' @export
`[.ArgList` <- function(x, argNames) {
  x$effective_args[argNames]
}

#' @export
`names.ArgList` <- function(x) {
  names(x$effective_args)
}

#' Create a ArgList isntance
#' 
#' Create a ArgList isntance from the calling environment, normally within an api function with a lengthy argument list.
#' @param envir an environment, default as the calling environment
#' @param default_args A list of default arguments
#' @return ArgList instance
#' @export
newArgList = function(envir = parent.frame(), default_args = list(), ellipsis_func = list) {
  # Get explicitly passed-in args from parent's match.call() formals
  explicitArgs = as.list(do.call(match.call, list(), envir = envir)[-1])
  # tripleDot = do.call(list, list(quote(...)), envir = envir)
  unexpandedArgs = do.call(match.call, list(expand.dots = FALSE), envir = envir)
  tripleDot = if(!is.null(unexpandedArgs[['...']])) do.call(list, list(quote(...)), envir = envir) else NULL
  # Force evaluate non-missing args
  for(argName in names(explicitArgs)){
    # If arg is not missing OR arg is a triple-dot arg (..1, ..2)
    if(do.call(hasArg, list(argName), envir = envir)){
      # argName can be passed in as a part of the ... from another function call, in which case eval(..1) fails
      # but get the 'argName' by name will work because ... args must be passed in as named args.
      explicitArgs[[argName]] <- if(hasName(tripleDot, argName)){
          # argName is never defined formally, so it's wrapped in ...
          tripleDot[[argName]]
      } else {
        # argName exists in the inner-most caller function,
        #but not defined formally in the parent function of the inner-most caller function
        eval(as.name(argName), envir = envir)
      }
    }
    else{
      # If not hasArg(argName), remove it from the explicitArgs even it shows up in match.call().
      # This happens when there are multiple nested function calls with passing missing arguments
      explicitArgs[[argName]] <- NULL
    }
  }
  # Find non-explicit default args (ie. missing args but with a default value)
  defaultArgs = default_args[!names(default_args) %in% names(explicitArgs)]
  return(ArgList$new(explicit_args = explicitArgs,
                 effective_args = c(explicitArgs, defaultArgs)))
}
