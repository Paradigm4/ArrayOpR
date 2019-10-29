WriteOp  <- R6::R6Class("WriteOp",
  inherit = ArrayOpBase,
  private = list(target = NULL, dataset = NULL, append = NULL, redimension = NULL),
  active = NULL,
  
  public = list(
    initialize = function(target, dataset, append = TRUE, redimension = TRUE) {
      # Validate dataset fields match
      missingTargetFields = dataset$get_absent_fields(target$dims_n_attrs)
      assert(!.has_len(missingTargetFields), "WriteOp dataset doesn't have target fields: %s", paste(missingTargetFields, collapse = ', '))
      
      private$target = target
      private$dataset = dataset
      private$append = append
      private$redimension = redimension
      # callSuper(target = target, dataset = dataset, append = append, redimension = redimension)
    }
    , .raw_afl = function(){
      targetArray = private$target$to_afl()
      inner = if(private$redimension) .afl(private$dataset$to_afl() %redimension% targetArray) else private$dataset$to_afl()
      if(private$append)
        .afl(inner %insert% targetArray)
      else
        .afl(inner %store% targetArray)
    }
  )
)

get_aio_op = function(filepath, template, aio_settings = list(), field_conversion = NULL, field_types){
  #' Get an CustomizedOp that generates AFL for aio_input
  #'
  #' @param filepath A single file path
  #' @param template An ArrayOp sub-class used as an template for fields detection and matching
  #' @param aio_settings Customized settings of aio_input
  #' @param field_conversion If NULL (default), use template's field type to convert aio_input attributes; Or provide
  #' a list for customized field conversion
  #' @param field_types Instead of using a template, explicitly specify a field_types list. Cannot be used with template
  #'
  #' @return CustomizedOp ready to be used in a WriteOp as the dataset

  assert(xor(methods::hasArg('template'), methods::hasArg('field_types')),
         "Only one of the 'template' and 'field_types' argument can exist!")

  if(methods::hasArg('template')){
    assert(inherits(template, c('ArrayOpBase')),
           "get_aio_op: unknown template class '%s'. Must be ArrayOpBase sub-class", class(template))
    fieldTypes = template$get_field_types(template$dims_n_attrs)
  } else {
    fieldTypes = field_types
  }

  # Populate aio settings
  aio_settings = c(list(path = filepath, num_attributes = length(fieldTypes)), aio_settings)
  settingItems = mapply(function(x, name) sprintf("'%s=%s'", name, x), aio_settings, names(aio_settings))

  # cast raw attributes
  castedItems = mapply(function(ft, index, name){
    fmt = if(ft == 'string') "%s" else paste0(ft, "(%s)")
    # If there is customized field conversion, use it
    attrName = sprintf("a%s", index)
    if(!is.null(field_conversion) && !is.null(field_conversion[[name]]))
        # If customized field conversion defined for 'name', then use it
       gsub('@', attrName, field_conversion[[name]])  # Replace all @ occurences in template
    else # Otherwise just directly 'cast' it to the right data type if needed.
      sprintf(fmt, attrName)
  }, fieldTypes, 1:length(fieldTypes) - 1, names(fieldTypes))

  aioExpr = sprintf("aio_input(%s)", .afl_join_fields(settingItems))
  applyExpr = .afl(aioExpr %apply% .afl_join_fields(names(fieldTypes), castedItems))
  projectedExpr = .afl(applyExpr %project% .afl_join_fields(names(fieldTypes)))
  return(CustomizedOp$new(projectedExpr))
}

# private functions -----------------------------------------------------------------------------------------------

