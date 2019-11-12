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
ArrayOpV18 <- R6::R6Class("ArrayOpV18",
  inherit = ArrayOp,
  private = list(
    validate_join_operand = function(side, operand, keys){
      assert(inherits(operand, 'ArrayOpV18'),
        "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
        side, 'ArrayOpBase', class(operand))
      assert(is.character(keys) && .has_len(keys),
        "Join arg 'on_%s' must be a non-empty R character, but got '%s' instead", side, class(keys))
      absentKeys = operand$get_absent_fields(keys)
      assert_not_has_len(absentKeys, "JoinOp arg 'on_%s' has invalid fields: %s", side, paste(absentKeys, collapse = ', '))
    }
  )
  ,
  active = NULL,
  
  # Public ----------------------------------------------------------------------------------------------------------
  public = list(
    
    join = function(right, on_left, on_right, settings = NULL, 
      dim_mode = 'keep', artificial_field = .random_attr_name()) {
      # Validate left and right
      left = self
      private$validate_join_operand('left', left, on_left)
      private$validate_join_operand('right', right, on_right)
      
      # Assert left and right keys lengths match
      assert(length(on_left) == length(on_right),
        "ERROR: ArrayOp$join: on_left[%s field(s)] and on_right[%s field(s)] must have the same length.",
        length(on_left), length(on_right))
      
      # Validate settings
      if(.has_len(settings)){
        settingKeys <- names(settings)
        if(!.has_len(settingKeys) || any(settingKeys == '')){
          stop("ERROR: ArrayOp$join: Settings must be a named list and each setting item must have a non-empty name when creating a JoinOp")
        }
      }
      # Validate selected fields
      hasSelected = .has_len(left$selected) || .has_len(right$selected)
      if(hasSelected && !.has_len(left$selected))
        assert(right$selected != on_right, 
          "ERROR: ArrayOp$join: Right operand's selected field(s) '%s' cannot be its join key(s) '%s' when there is no left operand fields selected.
Please select on left operand's fields OR do not select on either operand. Look into 'equi_join' documentation for more details.",
          paste(right$selected, collapse = ','), paste(on_right, collapse = ','))
      
      # Create setting items
      mergedSettings <- list(
        'left_names' = paste(on_left, collapse = ','),
        'right_names' = paste(on_right, collapse = ',')
      )
      mergedSettings <- c(mergedSettings, settings)
      # Values of setting items cannot be quoted.
      # But the whole 'key=value' needs single quotation according to equi_join plugin specs
      settingItems = mapply(function(key, value) {
        sprintf("'%s=%s'", key, value)
      }, names(mergedSettings), mergedSettings)
      keep_dimensions = (function(){
        val = settings[['keep_dimensions']]
        .has_len(val) && val == 1
      })()
      
      # Join two operands
      joinExpr <- sprintf("equi_join(%s, %s, %s)",
        left$to_join_operand_afl(on_left, keep_dimensions = keep_dimensions, artificial_attr = artificial_field), 
        right$to_join_operand_afl(on_right, keep_dimensions = keep_dimensions, artificial_attr = artificial_field),
        paste(settingItems, collapse = ', '))
      
      
      dims = list(instance_id = 'int64', value_no = 'int64')
      attrs = (function() {
        if(hasSelected){
          rightSelected = base::setdiff(right$selected, on_right)  # Right key(s) are ignored/masked in equi_join
          as.character(unique(c(left$selected, rightSelected)))
        }
        else{
          leftRetained = if(keep_dimensions) left$attrs_n_dims else left$attrs
          rightRetained = if(keep_dimensions) right$attrs_n_dims else right$attrs
          return(c(on_left,
            base::setdiff(leftRetained, on_left),
            base::setdiff(rightRetained, on_right)
          ))
        }
      }) ()
      dtypes = plyr::compact(c(dims, c(left$dtypes, right$dtypes)[attrs]))
      selectedFields = if(hasSelected) attrs else NULL
      joinedOp = self$new(joinExpr, names(dims), attrs, dtypes = dtypes)
      if(hasSelected) {
        joinedOp$reshape(select = selectedFields, dim_mode = dim_mode, artificial_field = artificial_field)
      }
      else joinedOp
    }
  )
)

