#' SubsetOp
#' @description 
#' Subset array content by 1. filtering cells 2. select only desired dimensions/attributes
#' @details 
#' SubsetOp details to be added here
#' @export
SubsetOp <- R6::R6Class("SubsetOp",
  inherit = ArrayOpBase,

  # Because we cannot 'project' array dimensions (attributes only), when no attr is selected,
  # we have to create an artificial attr to get rid of existing attrs.
  private = list(operand = NULL, filter_expr = NULL, .selected = NULL),
  
  active = list(
    dims = function() private$operand$dims,
    attrs = function() {
      if(.has_len(self$selected))
        base::intersect(private$operand$attrs, self$selected)
      else
        private$operand$attrs
    },
    selected = function() private$.selected
  ),

  public = list(

    initialize = function(operand, filter_expr = NULL, selected_fields = as.character(c())
      , errorTemplate = "SubsetOp missing field(s): '%s'"
      ){
      # Validate args
      assert(inherits(operand, 'ArrayOpBase'),
        "SubsetOp operand must be an ArrayOpBase sub-class, but got: %s", class(operand))
      assert(is.character(selected_fields) || is.null(selected_fields),
        "SubsetOp selected_fields must be a character vector or NULL, but got: %s", class(selected_fields))
      absentFields = operand$get_absent_fields(selected_fields)
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
      
      private$operand = operand
      private$filter_expr = singleFilterExpr
      private$.selected = selected_fields

      # After calling super initialize function, self is available
      info = structure(list(operand$get_field_types(self$dims_n_attrs)), names = c(.DT))
      super$initialize(info = info)
    }
    ,
    .raw_afl = function(){
      if(is.null(private$filter_expr))
        return(private$operand$to_afl())
      return(afl(private$operand %filter% afl_filter_from_expr(private$filter_expr)))
    }
    ,
    #' @description 
    #' Create a new SubsetOp using self as a template.
    #'
    #' Nothing except the `selected_fields` is changed.
    #' @param fields selected_fields in the new SubsetOp
    #' @param replace whehter `fields` replaces or supplements the `selected_fields`
    select_copy = function(fields, replace = TRUE) {
      
      extraFields = fields[!is.element(fields, selected_fields)]
      combinedFields = if(replace) fields else c(selected_fields, extraFields)
      return(SubsetOp(private$operand, private$filter_expr, combinedFields))
    }
  )
)

