CustomizedOp  <- setRefClass(
  "CustomizedOp",
  contains = "ArrayOpBase",
  fields = c('afl_expr', 'dims', 'attrs', 'selected', 'validate_fields'),
  methods = list(
    initialize = function(afl_expr, dims=NULL, attrs=NULL, selected = NULL,
                          field_types = list(), validate_fields = F, ...) {

      info = list(...)
      info[[.DT]] <- field_types
      callSuper(afl_expr = afl_expr, dims = dims, attrs = attrs,
                selected = selected, validate_fields = validate_fields,
                .info = info)
    }
    , .get_dimension_names = function() dims
    , .get_attribute_names = function() attrs
    , .get_selected_names = function() selected
    , .raw_afl = function() afl_expr
    , get_absent_fields = function(fieldNames, type = .OWN) {
      if(!validate_fields) return(as.character(NULL))
      return(callSuper(fieldNames, type))
    }
  )
)
