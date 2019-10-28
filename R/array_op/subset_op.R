SubsetOp <- setRefClass("SubsetOp", contains = 'ArrayOpBase',

  # Because we cannot 'project' array dimensions (attributes only), when no attr is selected,
  # we have to create an artificial attr to get rid of existing attrs.
  fields = c('operand', 'filter_expr', 'selected_fields', 'artificial_attr'),

  methods = list(

    initialize = function(operand, filter_expr = NULL, selected_fields = as.character(c())
      , errorTemplate = "SubsetOp missing field(s): '%s'"
      , artificial_attr = .random_attr_name()
      ){
      # Validate args
      assert(inherits(operand, 'ArrayOpBase'),
        "SubsetOp operand must be an ArrayOpBase sub-class, but got: %s", class(operand))
      assert(is.character(selected_fields) || is.null(selected_fields),
        "SubsetOp selected_fields must be a character vector or NULL, but got: %s", class(selected_fields))
      absentFields = operand$get_absent_fields(selected_fields, type = .OWN)
      assert_not_has_len(absentFields, errorTemplate, paste(absentFields, collapse = ', '))

      # For convenience, filter_expr can be NULL, a single R Expression, or a list of R Expressions
      # In the last case, all Expressions will be merged into a single one using 'AND' operator
      assert(is.call(filter_expr) || is.null(filter_expr) || is.list(filter_expr),
        "SubsetOp filter_expr must be an R call expression (or a list of R Expr) or NULL, but got: %s", class(filter_expr))
      singleFilterExpr = if(is.list(filter_expr)){
        assert(all(sapply(filter_expr, is.call)), "All elements in 'filter_expr' must be R Expr")
        e_merge(filter_expr)
      } else filter_expr

      # Validate the filter_expr
      status = operand$validate_filter_expr(singleFilterExpr)
      if(!status$success){
        if(.has_len(status$absent_fields))
          stopf(errorTemplate, paste(status$absent_fields, collapse = ','))
        stop(paste(status$error_msgs, collapse = '\n'))
      }

      callSuper(operand = operand, filter_expr = singleFilterExpr, selected_fields = selected_fields,
        artificial_attr = artificial_attr, .info = NULL)

      # After calling super initialize function, .self is available
      info = structure(list(operand$get_field_types(.self$get_field_names(.OWN))), names = c(.DT))
      .self$.info = info
    }
  )
)


# Private methods -------------------------------------------------------------------------------------------------

SubsetOp$methods(
  # Depending on whether there is a filter_expr, the 'filtered operand' can be either an AFL filter operation or
  # the original operand's AFL array expressions.

  .raw_afl = function(){
    if(is.null(filter_expr))
      return(operand$to_afl())
    return(.afl(operand$to_afl() %filter% afl_filter_from_expr(filter_expr)))
  }
  , .get_dimension_names = function() operand$.get_dimension_names()
  , .get_attribute_names = function() {
    if(.has_len(selected_fields))
      base::intersect(operand$.get_attribute_names(), selected_fields)
    else
      operand$.get_attribute_names()
  }
  , .get_selected_names = function() selected_fields


  # For compatibility only
  , select_copy = function(fields, replace = TRUE) {
    #' Create a new SubsetOp using .self as a template.
    #'
    #' Nothing except the `selected_fields` is changed.
    #' @param fields selected_fields in the new SubsetOp
    #' @param replace whehter `fields` replaces or supplements the `selected_fields`

    extraFields = fields[!is.element(fields, selected_fields)]
    combinedFields = if(replace) fields else c(selected_fields, extraFields)
    return(SubsetOp(operand, filter_expr, combinedFields, artificial_attr))
  }
)
