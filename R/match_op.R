#' MatchOp
#' 
#' MatchOp filter an ArrayOp's row records (or cells in Scidb term) by a 'template'.
#' 
#' Features:
#' - No schema change (ie. same set of attrs/dims)
#' - 'template' can be an R data frame or another ArrayOp
#' - Can be implemented in differnt modes to optimize performance
#' @export
MatchOp <- R6::R6Class("MatchOp",
  inherit = ArrayOpBase,
  private = list(operand = NULL, afl = NULL),
  
  active = list(
    dims = function() private$operand$dims,
    attrs = function() private$operand$attrs,
    selected = function() private$operand$selected
  ), 

  public = list(
    # The ... arg can be used to pass customized setting in each op_mode where applicable
    initialize = function(mainOperand, template, op_mode = 'filter', on_left, on_right, ...) {
      assert(inherits(mainOperand, 'ArrayOpBase'),
             "MatchOp: main operand must be an ArrayOp, but got '%s'", class(mainOperand))
      assert(op_mode == 'customized' || inherits(template, c('data.frame', 'ArrayOpBase')),
             "MatchOp: template must be a data.frame or an ArrayOp, but got '%s'", class(template))

      selector = list(filter = .filter_mode, join = .join_mode, cross_between = .cross_between_mode,
                      customized = .customized_mode)
      func = selector[[op_mode]]
      assert(!is.call(func), "MatchOp: unknown op_mode '%s' ", op_mode)

      converted = func(mainOperand, template, op_mode = 'filter', on_left, on_right, ...)

      # super$initialize(operand = converted[['operand']], afl = converted[['afl_literal']],
      #           .info = converted[['operand']]$.info)
      
      private$operand = converted$operand
      private$afl = converted$afl_literal

    }
    ,
    get_field_types = function(...) private$operand$get_field_types(...)
    ,
    .raw_afl = function() private$afl

    #' #' MatchOp has the same schema as its operand. So all field queries are delegated to its operand.
    #' , get_field_names = function(...) operand$get_field_names(...)
    #' , .get_dimension_names = function() operand$.get_dimension_names()
    #' , .get_attribute_names = function() operand$.get_attribute_names()
    #' , .get_selected_names = function() operand$.get_dimension_names()

  )
)

.INT64_MAX = '9223372036854775807'
.INT64_MIN = '-9223372036854775807'


# filter mode -----------------------------------------------------------------------------------------------------

.filter_mode = function(mainOperand, template, op_mode = 'filter', on_left, on_right, ...){
  assert(inherits(template, 'data.frame'),
    "JoinOp filter mode only works with R data.frame template, but got: %s", class(template))
  unmatchedCols = base::setdiff(names(template), mainOperand$dims_n_attrs)
  assert_not_has_len(unmatchedCols, "MatchOp template fields '%s' missing in the main operand.",
    paste(unmatchedCols, collapse = ','))

  dtypes = sapply(template, class)
  needQuotes = dtypes != 'numeric'
  valueStrTemplates = lapply(needQuotes, .ifelse, "'%s'", "%s")

  # Lower and upper bounds
  otherArgs = list(...)
  lower_bound = if(methods::hasArg('lower_bound')) otherArgs[['lower_bound']] else NULL
  upper_bound = if(methods::hasArg('upper_bound')) otherArgs[['upper_bound']] else NULL

  convertRow = function(eachRow, colNames) {
    rowValues = mapply(sprintf, valueStrTemplates, eachRow)
    rowItems = mapply(function(name, val){
      if(name %in% lower_bound) operator = '>='
      else if(name %in% upper_bound) operator = '<='
      else operator = '='
      sprintf("%s%s%s", name, operator, val)
    }, colNames, rowValues)
    sprintf(
      .ifelse(length(rowValues) > 1, "(%s)", "%s"), # String template for a row
      paste(rowItems, collapse = ' and ')
    )
  }

  filter_afl = paste( apply(template, 1, convertRow, names(template)), collapse = ' or ' )
  afl_literal = sprintf("filter(%s, %s)", mainOperand$to_afl(), filter_afl)
  return(list(operand = mainOperand, afl_literal = afl_literal))
}


# join mode -------------------------------------------------------------------------------------------------------

.join_mode = function(mainOperand, template, op_mode = 'join', on_left, on_right, ...){
  templateSchema = if(inherits(template, 'data.frame')) DEP$df_to_arrayop_func(template)
  else if(inherits(template, 'ArrayOpBase')) template
  else stopf("MatchOp: unknown template class '%s' in join mode", class(template))
  if(.has_len(templateSchema$selected))
    templateSchema = templateSchema$select_copy(NULL, replace = TRUE)
  if(methods::hasArg('on_left') && methods::hasArg('on_right')){
    joinOp = JoinOp$new(mainOperand, templateSchema, on_left = on_left, on_right = on_right, ...)
  }
  else{
    keys = base::intersect(templateSchema$dims_n_attrs, mainOperand$dims_n_attrs)
    assert_has_len(keys,
      "MatchOp join mode: none of the template fields matches the main operand: '%s'",
      paste(templateSchema$dims_n_attrs, collapse = ','))
    joinOp = JoinOp$new(mainOperand, templateSchema, on_left = keys, on_right = keys, ...)
  }
  afl_literal = joinOp$.raw_afl()
  return(list(operand = joinOp, afl_literal = afl_literal))
}


# cross_between mode ----------------------------------------------------------------------------------------------


.cross_between_mode = function(mainOperand, template, op_mode = 'cross_between', on_left, on_right, ...){
  templateSchema = if(inherits(template, 'data.frame')) DEP$df_to_arrayop_func(template)
  else if(inherits(template, 'ArrayOpBase')) template
  else stopf("MatchOp: unknown template class '%s' in cross_between mode", class(template))

  explicit = methods::hasArg('on_left') && methods::hasArg('on_right')

  mainDims = mainOperand$dims

  if(explicit){
    dimMatchMarks = mainDims %in% on_left
    matchedDims = structure(on_right, names = on_left)
  } else {
    dimMatchMarks = mainDims %in% templateSchema$dims_n_attrs
    matchedDims = base::intersect(templateSchema$dims_n_attrs, mainDims)
    matchedDims = structure(matchedDims, names = matchedDims)
  }

  assert_has_len(matchedDims,
    "MatchOp cross_between: none of the template fields matches the main operand's dimensions: '%s'",
    paste(templateSchema$dims_n_attrs, collapse = ','))

  # get region array's attr values
  getRegionArrayAttrValue = function(default){
    res = rep(default, length(mainDims))
    for(i in 1:length(mainDims)){
      if(dimMatchMarks[[i]]){
        mainDimKeyName = mainDims[[i]]
        res[[i]] <- sprintf("int64(%s)", matchedDims[[mainDimKeyName]])
      }
    }
    return(res)
  }

  regionLowAttrValues = getRegionArrayAttrValue(.INT64_MIN)
  regionHighAttrValues = getRegionArrayAttrValue(.INT64_MAX)

  regionLowAttrNames = sprintf('_%s_low', mainDims)
  regionHighAttrNames = sprintf('_%s_high', mainDims)

  # apply new attributes as the region array in 'cross_between'
  applyExpr = paste(regionLowAttrNames, regionLowAttrValues, regionHighAttrNames, regionHighAttrValues,
    sep = ',', collapse = ',')
  afl_literal = afl(
    mainOperand %cross_between%
      afl(templateSchema %apply% applyExpr %project%
          c(regionLowAttrNames, regionHighAttrNames))
  )
  return(list(operand = mainOperand, afl_literal = afl_literal))
}


# Customized mode -------------------------------------------------------------------------------------------------

.customized_mode = function(mainOperand, template, op_mode = 'customized', on_left, on_right, ...){
  assert(methods::hasArg('afl'), "MatchOp customized mode must have an 'afl' argument.")
  afl_literal = list(...)[['afl']]
  return(list(operand = mainOperand, afl_literal = afl_literal))
}
