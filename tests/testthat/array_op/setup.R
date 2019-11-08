# A sub-class of ArrayOpBase
# Fields management/validation is the detail of sub classes.
# Here we use AnyArrayOp to mimic any potentail sub-classes of ArrayOpBase and
#   to the basic behavior that ArrayOpBase provides disregard of sub-class details.

AnyArrayOp  <- R6::R6Class("AnyArrayOp",
  inherit = ArrayOpBase,
  private = list(array_expr = NULL, .dims = NULL, .attrs = NULL, .selected = NULL),
  
  active = list(
    dims = function() private$.dims,
    attrs = function() private$.attrs,
    selected = function() private$.selected
  ),
  
  public = list(
    initialize = function(array_expr, dims, attrs, selected = NULL) {
      private$array_expr = array_expr
      private$.dims = dims
      private$.attrs = attrs
      private$.selected = selected
      
      fields = self$dims_n_attrs
      dtypes = as.list(sapply(fields, function(x) sprintf("dt_%s", x)), USE.NAMES = TRUE)
      super$initialize(info = structure(list(dtypes), names = .DT))
    }
    ,
    .raw_afl = function() private$array_expr
  )
)


# Mock scidb upload and return an ArraySchema

DEP$df_to_arrayop_func = function(df){
  address = data.table::address(df)
  ArraySchema$new(address, namespace = NULL, dims = 'i', attrs = names(df))
}


# Add constants for tests

EMPTY_NAMED_LIST = structure(as.list(NULL), names = as.character(c()))

