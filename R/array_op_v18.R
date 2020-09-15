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
#' @noRd
ArrayOpV18 <- R6::R6Class("ArrayOpV18",
  inherit = ArrayOpBase,
  cloneable = FALSE,
  private = list(
    equi_join_template = function(left_alias, right_alias) {
      "equi_join(%s, %s, %s)"
    }
    ,
    to_equi_join_setting_item_str = function(key, value, left_alias, right_alias) {
      valueStr = if(length(value) > 1) paste(value, collapse = ',') else value
      sprintf("'%s=%s'", key, valueStr)
    }
    ,
    to_aio_setting_item_str = function(key, value) {
      valueStr = if(length(value) > 1) paste(value, collapse = ';') else value
      if(key == 'path' && length(value) > 1) key = 'paths'
      sprintf("'%s=%s'", key, valueStr)
    }
  )
  ,
  active = NULL,
  
  # Public ----------------------------------------------------------------------------------------------------------
  public = list(
    
  )
)

