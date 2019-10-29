CustomizedOp  <- R6::R6Class("CustomizedOp",
  inherit = ArrayOpBase,
  
  private = list(afl_expr = NULL, .dims = NULL, .attrs = NULL, .selected = NULL, validate_fields = NULL),
  
  active = list(
    dims = function() private$.dims,
    attrs = function() private$.attrs,
    selected = function() private$.selected
  ),
  
  public = list(
    initialize = function(afl_expr, dims=NULL, attrs=NULL, selected = NULL,
                          field_types = list(), validate_fields = F, ...) {

      private$afl_expr = afl_expr
      private$.dims = dims
      private$.attrs = attrs
      private$.selected = selected
      private$validate_fields = validate_fields
      
      info = list(...)
      info[[.DT]] <- field_types
      super$initialize(info)
    }
    , .raw_afl = function() private$afl_expr
    , get_absent_fields = function(fieldNames) {
      if(!private$validate_fields) return(as.character(NULL))
      return(super$get_absent_fields(fieldNames))
    }
  )
)
