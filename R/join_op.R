

# Model a sql-like join oepration.
# Join on two operands whose class is SubsetOp or JoinOp. Join on another JoinOp enables multiple equi_join operations.
# The constructor function performs validation.
JoinOp <- R6::R6Class("JoinOp", 
  inherit = ArrayOpBase,
  
  active = list(
    # Resultant join expression does not have meaningful dimensions.
    # instance_id,value_no are generated automatically by equi_join
    dims = function() c('instance_id', 'value_no'),
    
    # All selected fields are attributes
    attrs = function() {
      if(.has_len(private$left$selected) || .has_len(private$right$selected))
        return(self$selected)
      else{
        leftRetained = if(self$.keep_dimensions()) private$left$attrs_n_dims else private$left$attrs
        rightRetained = if(self$.keep_dimensions()) private$right$attrs_n_dims else private$right$attrs
        return(c(private$on_left,
          base::setdiff(leftRetained, private$on_left),
          base::setdiff(rightRetained, private$on_right)
        ))
      }
    },
    
    # Combine the selected of left and right
    selected = function() {
      
      # equi_join 'on_right' keys are omitted, only the 'on_left' keys are retained
      rightSelected = base::setdiff(private$right$selected, private$on_right)
      as.character(unique(c(private$left$selected, rightSelected)))
    }
  )
  ,
  private = list(left = c(), right = c(), on_left = c(), on_right = c(), settings = c(),
    validate_join_operand = function(side, operand, keys){
      assert(inherits(operand, 'ArrayOpBase'),
        "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
        side, 'ArrayOpBase', class(operand))
      assert(is.character(keys) && .has_len(keys),
        "Join arg 'on_%s' must be a non-empty R character, but got '%s' instead", side, class(keys))
      absentKeys = operand$get_absent_fields(keys)
      assert_not_has_len(absentKeys, "JoinOp arg 'on_%s' has invalid fields: %s", side, paste(absentKeys, collapse = ', '))
    }
  )
  ,
  public = list(
    initialize = function(left, right, on_left, on_right, settings = list()) {
      # Validate left and right
      private$validate_join_operand('left', left, on_left)
      private$validate_join_operand('right', right, on_right)
      # Assert left and right keys lengths match
      assert(length(on_left) == length(on_right),
        "JoinOp on_left[%s field(s)] and on_right[%s field(s)] must have the same length.",
        length(on_left), length(on_right))

      # Validate settings
      if(.has_len(settings)){
        settingKeys <- names(settings)
        if(!.has_len(settingKeys) || any(settingKeys == '')){
          stop("Settings must be a named list and each setting item must have a non-empty name when creating a JoinOp")
        }
      }
      
      private$left = left
      private$right = right
      private$on_left = on_left
      private$on_right = on_right
      private$settings = settings

      # After calling super initialize function, self is available
      fieldTypes = c(
        list(left$get_field_types(self$attrs)),
        list(right$get_field_types(self$attrs))
      )
      super$initialize(info = structure(fieldTypes, names = c(.DT)))
    },
    
    #' @description 
    #' Return AFL when self used as an operand in another parent operation.
    #' 
    #' Implemented by calling to_afl_explicit with `selected_fields = self$selected`
    #'
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' By default, dimensions are not dropped in parent operation
    #' But in some operations, dimensions are dropped or converted to attributes
    #' e.g. equi_join creates two artificial dimensions and discard any existing dimensions of two operands.
    # to_afl = function(drop_dims = !self$.keep_dimensions(), artificial_attr = .random_attr_name()) {
    #   super$to_afl(drop_dims = F, artificial_attr = artificial_attr)
    # },

    # Settings should be a named list
    # where the name is the setting key (e.g. algorithm, has_join_threshold)
    # and the value is setting value (e.g. hash_replicate_left, 1024)
    .raw_afl = function() {
      # left_names and right_names are mandatory settings syntactically,
      # but the JoinOp's setting field only stores extra settings excluding left_names and right_names
      mergedSettings <- list()
      mergedSettings[['left_names']] <- paste(private$on_left, collapse = ',')
      mergedSettings[['right_names']] <- paste(private$on_right, collapse = ',')
      mergedSettings <- c(mergedSettings, private$settings)
      # Values of setting items cannot be quoted.
      # But the whole 'key=value' needs single quotation according to equi_join plugin specs
      settingItems = mapply(function(key, value) sprintf("'%s=%s'", key, value), names(mergedSettings), mergedSettings)
      joinExpr <- sprintf("equi_join(%s, %s, %s)",
        private$left$to_join_operand_afl(private$on_left), private$right$to_join_operand_afl(private$on_right),
        paste(settingItems, collapse = ', '))

      return(joinExpr)
    }
    ,
    .keep_dimensions = function() {
      val = private$settings[['keep_dimensions']]
      return(!is.null(val) && val == 1)
    }
  )
)
