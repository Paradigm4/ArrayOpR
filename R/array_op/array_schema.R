# Model an array in SciDB
ArraySchema <- setRefClass("ArraySchema",
  # fields = list(array_name = 'vector', attributes = 'Field', dimensions = 'Field'),
  contains = 'ArrayOpBase',
  fields = c("operand", "attrs", "dims"),

  methods = list(
    initialize = function(array_name, attrs, dims, namespace = "public", info = NULL) {
      # Ensure every attribute/dimension inherits Field class

      fullName = if(is.null(namespace)) array_name else paste0(namespace, '.', array_name)

      callSuper(operand = fullName, attrs = attrs, dims = dims, .info = info)
    }
    , .raw_afl = function() operand

    # Private functions
    , .get_dimension_names = function() dims
    , .get_attribute_names = function() attrs
  )
)
