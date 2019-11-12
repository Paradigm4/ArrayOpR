# A base class for other specialized array operands/operations, e.g. SubsetOp, JoinOp, ArraySchema

# Meta key constants
# KEY.DIM = 'dims'
# KEY.ATR = 'attrs'
# KEY.SEL = 'selected'


#' Base class of all ArrayOp classes
#' @description 
#' ArrayOp classes denote scidb array operations and operands, hence the name. 
#' @details 
#' One operation consists of an scidb operator and [1..*] operands, of which the result can be used as an operand 
#' in another operation. Operands and Opreration results can all be denoted by ArrayOp.
#' @export
ArrayOpV19 <- R6::R6Class("ArrayOpV19",
  inherit = ArrayOp,
  private = NULL,
  active = NULL,
  
  # Public ----------------------------------------------------------------------------------------------------------
  public = list(
    initialize = function(...) {
      super$initialize(...)
    }
    ,
    join = function(right, on_left, on_right, settings = NULL) {
      'join_v19'
    }
  )
)

