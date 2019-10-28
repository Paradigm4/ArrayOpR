.validate_join_operand <- function(side, operand, keys){
  assert(inherits(operand, 'ArrayOpBase'),
    "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
    side, 'ArrayOpBase', class(operand))
  assert(is.character(keys) && .has_len(keys),
    "Join arg 'on_%s' must be a non-empty R character, but got '%s' instead", side, class(keys))
  absentKeys = operand$get_absent_fields(keys)
  assert_not_has_len(absentKeys, "JoinOp arg 'on_%s' has invalid fields: %s", side, paste(absentKeys, collapse = ', '))
}

# Model a sql-like join oepration.
# Join on two operands whose class is SubsetOp or JoinOp. Join on another JoinOp enables multiple equi_join operations.
# The constructor function performs validation.
JoinOp <- setRefClass("JoinOp", contains = 'ArrayOpBase',
  fields = c("left", "right", "on_left", "on_right", "settings"),

  methods = list(
    initialize = function(left, right, on_left, on_right, settings = list()) {
      # Validate left and right
      .validate_join_operand('left', left, on_left)
      .validate_join_operand('right', right, on_right)
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

      callSuper(left = left, right = right, on_left = on_left, on_right = on_right, settings = settings, .info = NULL)

      # After calling super initialize function, .self is available
      fieldTypes = c(
        list(left$get_field_types(.self$get_field_names(.ATR))),
        list(right$get_field_types(.self$get_field_names(.ATR)))
      )
      # fieldTypes = as.list(fieldTypes)
      info = structure(fieldTypes, names = c(.DT))
      .self$.info = info
    },

    # Settings should be a named list
    # where the name is the setting key (e.g. algorithm, has_join_threshold)
    # and the value is setting value (e.g. hash_replicate_left, 1024)
    .raw_afl = function() {
      # left_names and right_names are mandatory settings syntactically,
      # but the JoinOp's setting field only stores extra settings excluding left_names and right_names
      mergedSettings <- list()
      mergedSettings[['left_names']] <- paste(on_left, collapse = ',')
      mergedSettings[['right_names']] <- paste(on_right, collapse = ',')
      mergedSettings <- c(mergedSettings, settings)
      # Values of setting items cannot be quoted.
      # But the whole 'key=value' needs single quotation according to equi_join plugin specs
      settingItems = mapply(function(key, value) sprintf("'%s=%s'", key, value), names(mergedSettings), mergedSettings)
      joinExpr <- sprintf("equi_join(%s, %s, %s)",
        left$to_join_operand_afl(on_left), right$to_join_operand_afl(on_right),
        paste(settingItems, collapse = ', '))

      return(joinExpr)
    }
  )
)


JoinOp$methods(
  # Resultant join expression does not have meaningful dimensions.
  # instance_id,value_no are generated automatically by equi_join
  .get_dimension_names = function() c('instance_id', 'value_no'),

  # All selected fields are attributes
  .get_attribute_names = function() {
    if(.has_len(left$get_field_names(.SEL)) || .has_len(right$get_field_names(.SEL)))
      return(.get_selected_names())
    else{
      mode = if(.keep_dimensions()) .AD else .ATR
      return(c(on_left,
        base::setdiff(left$get_field_names(mode), on_left),
        base::setdiff(right$get_field_names(mode), on_right)
      ))
    }
  },

  # Combine the selected of left and right
  .get_selected_names = function() {

    # equi_join 'on_right' keys are omitted, only the 'on_left' keys are retained
    rightSelected = base::setdiff(right$get_field_names(.SEL), on_right)
    as.character(unique(c(left$get_field_names(.SEL), rightSelected)))
  },

  .keep_dimensions = function() {
     val = settings[['keep_dimensions']]
     return(!is.null(val) && val == 1)
  }
)
