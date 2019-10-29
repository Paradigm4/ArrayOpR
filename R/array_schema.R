# Model an array in SciDB
ArraySchema <- R6::R6Class("ArraySchema",
  inherit = ArrayOpBase,
  
  private = list(operand = NULL, .attrs = NULL, .dims = NULL),
  
  active = list(
    dims = function() private$.dims,
    attrs = function() private$.attrs
  ),

  public = list(
    initialize = function(array_name, attrs, dims, namespace = "public", info = NULL) {
      # Ensure every attribute/dimension inherits Field class

      fullName = if(is.null(namespace)) array_name else paste0(namespace, '.', array_name)

      private$operand = fullName
      private$.dims = dims
      private$.attrs = attrs
      super$initialize(info = info)
    }
    , .raw_afl = function() private$operand
  )
)
