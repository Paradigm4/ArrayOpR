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
  inherit = ArrayOpBase,
  private = list(
    to_setting_item_str = function(key, value) {
      valueStr = if(length(value) > 1) paste(value, collapse = ',') else value
      sprintf("'%s=%s'", key, valueStr)
    }
  )
  ,
  active = NULL,
  
  # Public ----------------------------------------------------------------------------------------------------------
  public = list(
    #' @description 
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
    #' @export
    load_file = function(filepath, aio_settings = list(), field_conversion = NULL, file_headers = NULL){
      
      if(!.has_len(file_headers))
        file_headers = self$dims_n_attrs

      lookup = structure(0:(length(file_headers) - 1), names = file_headers)
      colIndexes = vapply(self$dims_n_attrs, function(x) lookup[x], integer(1))
      colIndexes = colIndexes[!is.na(colIndexes)]

      fieldTypes = self$get_field_types(names(colIndexes))
      
      # Populate aio settings
      aio_settings = c(list(path = filepath, num_attributes = max(colIndexes) + 1), aio_settings)
      settingItems = mapply(private$to_setting_item_str, names(aio_settings), aio_settings)
      
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
      }, fieldTypes, colIndexes, names(fieldTypes))
      
      aioExpr = afl(afl_join_fields(settingItems) %aio_input% NULL)
      applyExpr = afl(aioExpr %apply% afl_join_fields(names(fieldTypes), castedItems))
      projectedExpr = afl(applyExpr %project% names(fieldTypes))
      return(self$create_new(projectedExpr, metaList = list()))
    }
  )
)

