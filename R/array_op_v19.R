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
  inherit = ArrayOpBase,
  private = list(
    equi_join_template = function(left_alias, right_alias) {
      sprintf("equi_join(%%s as %s, %%s as %s, %%s)", left_alias, right_alias)
    }
    ,
    to_equi_join_setting_item_str = function(key, value, left_alias, right_alias) {
      if(key == 'left_names')
        value = sprintf("%s.%s", left_alias, value)
      else if(key == 'right_names')
        value = sprintf("%s.%s", right_alias, value)
      valueStr = if(length(value) > 1) sprintf("(%s)", paste(value, collapse = ',')) else value
      sprintf("%s:%s", key, valueStr)
    }
    ,
    to_aio_setting_item_str = function(key, value) {
      if(is.character(value))
        value = sprintf("'%s'", value)  # Quote string value(s)
      valueStr = if(length(value) > 1) sprintf("(%s)", paste(value, collapse = ',')) else value
      if(key == 'path') 
        valueStr
      else 
        sprintf("%s:%s", key, valueStr)
    }
  )
  ,
  active = NULL
  ,
  # Public ----------------------------------------------------------------------------------------------------------
  public = list(
    
    
  )
)
