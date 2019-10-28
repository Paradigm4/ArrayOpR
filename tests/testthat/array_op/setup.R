# A sub-class of ArrayOpBase
# Fields management/validation is the detail of sub classes.
# Here we use AnyArrayOp to mimic any potentail sub-classes of ArrayOpBase and
#   to the basic behavior that ArrayOpBase provides disregard of sub-class details.

AnyArrayOp  <- setRefClass("AnyArrayOp",
  contains = "ArrayOpBase",
  fields = c('array_expr', 'dims', 'attrs', 'selected'),
  methods = list(
    initialize = function(array_expr, dims, attrs, selected = NULL) {
      callSuper(array_expr = array_expr, dims = dims, attrs = attrs, selected = selected, .info = NULL)
      fields = .self$get_field_names(.OWN)
      dtypes = as.list(sapply(fields, function(x) sprintf("dt_%s", x)), USE.NAMES = TRUE)
      .self$.info  = structure(list(dtypes), names = .DT)
    },
    .get_dimension_names = function() dims,
    .get_attribute_names = function() attrs,
    .get_selected_names = function() selected,

    .raw_afl = function() .self$array_expr
  )
)

assert_afl_equal <- function(actual, expected) {
  actual <- gsub('\\s+', '', actual)
  expected <- gsub('\\s+', '', expected)
  testthat::expect_identical(actual, expected)
}

# Mock scidb upload and return an ArraySchema

DEP$df_to_arrayop_func = function(df){
  address = data.table::address(df)
  ArraySchema(address, namespace = NULL, dims = 'i', attrs = names(df))
}


# Add constants for tests

EMPTY_NAMED_LIST = structure(as.list(NULL), names = as.character(c()))

