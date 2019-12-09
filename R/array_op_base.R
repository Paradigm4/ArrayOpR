# A base class for other specialized array operands/operations, e.g. SubsetOp, JoinOp, ArraySchema

MAX_DIM = '4611686018427387903'
MIN_DIM = '-4611686018427387902'


#' Base class of all ArrayOp classes
#' @description 
#' ArrayOp classes denote scidb array operations and operands, hence the name. 
#' @details 
#' One operation consists of an scidb operator and [1..*] operands, of which the result can be used as an operand 
#' in another operation. Operands and Opreration results can all be denoted by ArrayOp.
#' @export
ArrayOpBase <- R6::R6Class("ArrayOpBase",
  private = list(
    raw_afl = NULL
    ,
    metaList = NULL
    ,
    set_meta = function(key, value) {
      private$metaList[[key]] <- value
    }
    ,
    get_meta = function(key) {
      private$metaList[[key]]
    }
    ,
    assert_fields_exist = function(field_names, 
      errorMsgTemplate = sprintf("ERROR: Field(s) '%%s' not found in ArrayOp: %s", private$raw_afl)) {
      missingFields = base::setdiff(field_names, self$dims_n_attrs)
      assert_not_has_len(missingFields, 
        errorMsgTemplate, paste(missingFields, collapse = ',')
      )
    }
    ,
    validate_join_operand = function(side, operand, keys){
      assert(inherits(operand, class(self)),
        "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
        side, 'ArrayOpBase', class(operand))
      assert(is.character(keys) && .has_len(keys),
        "Join arg 'on_%s' must be a non-empty R character, but got '%s' instead", side, class(keys))
      absentKeys = operand$get_absent_fields(keys)
      assert_not_has_len(absentKeys, "JoinOp arg 'on_%s' has invalid fields: %s", side, paste(absentKeys, collapse = ', '))
    }
  ),
  active = list(
    dims = function() private$get_meta('dims'),
    attrs = function() private$get_meta('attrs'),
    selected = function() private$get_meta('selected'),
    dtypes = function() private$get_meta('dtypes'),
    dims_n_attrs = function() c(self$dims, self$attrs),
    attrs_n_dims = function() c(self$attrs, self$dims)
  ),


  # Public ----------------------------------------------------------------------------------------------------------

  public = list(
    #' @description 
    #' Base class initialize function, to be called in sub-class 
    #' @param info A list that stores ArrayOp meta data, e.g. field types 
    initialize = function(
      raw_afl,
      dims = as.character(c()), 
      attrs = as.character(c()),
      dtypes = list(),
      validate_fields = TRUE,
      dim_specs = list(),
      ...,
      metaList
    ) {
      assert(
        xor(methods::hasArg('metaList'), 
              methods::hasArg('dims') || methods::hasArg('attrs') || 
              methods::hasArg('validate_fields') || methods::hasArg('dtypes')
        ),
      "ERROR: ArrayOp:initialze: metaList cannot be provided with any of the args: dims, attrs, validate_fields, dtypes")
      private$raw_afl = raw_afl
      private$metaList = if(methods::hasArg('metaList')) metaList else
        list(dims = dims, attrs = attrs, dtypes = dtypes, validate_fields = validate_fields, dim_specs = dim_specs, ...)
    }
    ,
    #' @description 
    #' Return ArrayOp field types
    #'
    #' NOTE: private$info has to be defined, otherwise returns NULL
    #' @param field_names R character
    #' @return a named list as `field_names`, where absent fields or fields without data types are dropped silently.
    get_field_types = function(field_names = NULL){
      if(is.null(field_names))
        field_names = self$dims_n_attrs
      missingFields = base::setdiff(field_names, self$dims_n_attrs)
      assert_not_has_len(missingFields, 
        "ERROR: ArrayOp$get_field_types: field_names: Field(s) '%s' not found in ArrayOp: %s", 
        paste(missingFields, collapse = ','),
        private$raw_afl
      )
      missingFields = base::setdiff(field_names, names(self$dtypes))
      assert_not_has_len(missingFields, 
        "ERROR: ArrayOp$get_field_types: Field(s) '%s' not annotated with dtype in ArrayOp: %s", 
        paste(missingFields, collapse = ','),
        private$raw_afl
      )
      return(self$dtypes[field_names])
    }
    ,
    #' @description 
    #' Get dimension specifications
    #' 
    #' A dimension spec str is formatted as "lower bound : upper bound : overlap : chunk_length", 
    #' as seen in scidb `show(array_name)` operator.
    #' All dimensions' data types are int64. 
    #' @param dim_names Default NULL equals all dimensions.
    #' @return A named list where the name is dimension name and value is a dimension spec string
    get_dim_specs = function(dim_names = NULL) {
      if(!.has_len(dim_names)) dim_names = self$dims
      private$get_meta('dim_specs')[dim_names]
    }
    ,
    #' @description 
    #' Validate fields existence according the 'type' which defaults to 'owned' fields
    #' 
    #' This function is useful for validating fields in use cases:
    #    1. whether fields are valid for 'select' or 'filter';
    #    2. whether fields can be used as keys in JoinOp
    get_absent_fields = function(fieldNames) {
      if(!private$get_meta('validate_fields'))
        return(NULL)
      absentMarks = !(fieldNames %in% self$dims_n_attrs)
      return(fieldNames[absentMarks])
    }
    # Functions that creat new ArrayOps -------------------------------------------------------------------------------
    ,
    # Return the class constructor function. 
    # Should work in sub-class without requiring class names or constructor function.
    create_new = function(...){
      classConstructor = get(class(self))$new
      classConstructor(...)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance of the same class
    #' 
    #' The new instance shares all meta data with the template
    create_new_with_same_schema = function(new_afl) {
      self$create_new(new_afl, metaList = private$metaList)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance by using a filter expression on the parent ArrayOp
    #' 
    #' Similar to SQL where clause.
    #' @param missing_fields_error_template Error template for missing fields. 
    #' Only one %s is allowed which is substituted with an concatnation of the missing fields separated by commas.
    where = function(..., expr, missing_fields_error_template = NULL) {
      filterExpr = if(methods::hasArg('expr')) expr else e_merge(e(...))
      status = validate_filter_expr(filterExpr, self$dims_n_attrs)
      if(!status$success){
        if(.has_len(status$absent_fields)){
          if(is.null(missing_fields_error_template))
            missing_fields_error_template = 
              sprintf("ERROR: ArrayOp$where: Field(s) '%%s' not found in ArrayOp: %s", private$raw_afl)
          stopf(missing_fields_error_template, paste(status$absent_fields, collapse = ','))
        }
        stop(paste(status$error_msgs, collapse = '\n'))
      }
      newRawAfl = afl(self %filter% afl_filter_from_expr(filterExpr))
      self$create_new_with_same_schema(newRawAfl)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with selected fields
    #' 
    #' NOTE: this does NOT change the to_afl output, but explicitly state which field(s) are retained if used in
    #' a parent operation that changes its schema, e.g. equi_join or to_df(only_attributes = T)
    #' @param ... Which field(s) are retained during a schema-change operation
    select = function(...) {
      fieldNames = c(...)
      assert(is.character(fieldNames) || is.null(fieldNames), 
        "ERROR: ArrayOp$select: ... must be a character or NULL, but got: %s", fieldNames)
      private$assert_fields_exist(fieldNames, "ArrayOp$select")
      newMeta = private$metaList
      newMeta[['selected']] <- fieldNames
      self$create_new(private$raw_afl, metaList = newMeta)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with a different schema/shape
    #' 
    #' @param select a non-empty list, where named items are new derived attributes and
    #' unamed string values are existing dimensions/attributes.
    #' @param dtypes a named list to provide field data types for newly derived fields
    #' @param artificial_field A field name used as the artificial dimension name in `unpack` scidb operator
    #' By default, a random string is generated.
    reshape = function(select=NULL, dtypes = NULL, dim_mode = 'keep', artificial_field = .random_attr_name()) {
      if(dim_mode == 'keep'){
        assert(.has_len(select),
          "ERROR: ArrayOp$reshape: dim_mode='keep': 'select' param must be a non-empty character list, but got: %s", 
          class(select))
      }
      
      # Plain selected fields without change
      existingFields = if(.has_len(names(select))) select[names(select) == ''] else {
        as.character(select)
      }
      # Names of the retained fields (existing or new)
      selectFieldNames = .ifelse(!.has_len(names(select)), as.character(select),
        mapply(function(name, value){
          if(name == '') value else name
        }, names(select), select, USE.NAMES = F)
      )
      fieldExprs = select[names(select) != '']
      fieldNamesWithExprs = names(fieldExprs)
      
      newFieldNames = fieldNamesWithExprs %-% .ifelse(dim_mode=='keep', self$attrs, self$dims_n_attrs)
      newFields = fieldExprs[newFieldNames]
      
      replacedFieldNames = fieldNamesWithExprs %-% newFieldNames
      replacedFieldNamesAlt = sprintf("_%s", replacedFieldNames)
      replacedFields = sapply(replacedFieldNames, function(x) gsub('@', x, fieldExprs[[x]]))
      
      mergedDtypes = utils::modifyList(self$dtypes, as.list(dtypes))
      
      keep = function(){
        attrs = selectFieldNames %-% self$dims
        
        newAfl = if(.has_len(replacedFieldNames)){
          afl(self %apply% afl_join_fields(replacedFieldNamesAlt, replacedFields)
            %project% (attrs %-% newFieldNames %-% replacedFieldNames %u% replacedFieldNamesAlt) 
            %apply% afl_join_fields(replacedFieldNames %u% newFieldNames, replacedFieldNamesAlt %u% newFields))
        }
        else if(.has_len(attrs)) {
          inner = self
          if(.has_len(newFieldNames))
             inner = afl(self %apply% afl_join_fields(newFieldNames, newFields))
          afl(inner %project% attrs)
        }
        else {
          attrs = artificial_field
          afl(self %apply% c(artificial_field, 'null') %project% artificial_field)
        }
        self$create_new(newAfl, self$dims, attrs, mergedDtypes, validate_fields = private$get_meta('validate_fields'))
      }
      
      drop = function() {
        unpacked = afl(self %unpack% artificial_field)
        newAfl = if(.has_len(newFieldNames)) 
          afl(unpacked %apply% afl_join_fields(newFieldNames, newFields) %project% selectFieldNames) 
        else if(.has_len(selectFieldNames)) afl(unpacked %project% selectFieldNames)
        else unpacked
        # If no selected fields, then follow the unpack operator's default behavior:
        # all dimensions and attributes are converted to attributes
        if(!.has_len(selectFieldNames))
          selectFieldNames = self$dims_n_attrs
        
        newDtypes = c(mergedDtypes[selectFieldNames], structure('int64', names = artificial_field))
        self$create_new(newAfl, artificial_field, selectFieldNames, 
          dtypes = newDtypes, validate_fields = private$get_meta('validate_fields'))
      }
      
      ignore_in_parent = function() {
        identity
      }
      
      switch (dim_mode,
        'keep' = keep,
        'drop' = drop,
        # 'ignore_in_parent' = ignore_in_parent,
        stopf("ERROR: ArrayOp$reshape: invalid 'dim_mode' %s.", dim_mode)
      ) ()
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance by joining with another ArrayOp 
    #' 
    #' Currently implemented with scidb `equi_join` operator.
    #' @param right The other ArrayOp instance to join with.
    #' @param on_left R character vector. Join keys from the left (self).
    #' @param on_right R character vector. Join keys from the `right`. Must be of the same length as `on_left`
    #' @param .auto_select default: FALSE. If set to TRUE, the resultant ArrayOp instance will auto `select` the fields
    #' that are selected in the left and right operands but not in the right join keys (since they are masked by equi_join operator).
    #' @param settings `equi_join` settings, a named list where both key and values are strings. 
    #' @param .dim_mode How to reshape the resultant ArrayOp. Same meaning as in `ArrayOp$reshape` function. 
    #' By default, dim_mode = 'keep', the artificial dimensions, namely `instance_id` and `value_no` from `equi_join`
    #' are retained. If set to 'drop', the artificial dimensions will be removed. See `ArrayOp$reshape` for more details.
    #' @param .artificial_field As in `ArrayOp$reshpae`, it defaults to a random field name. It can be safely ignored in
    #' client code. It exists only for test purposes. 
    join = function(right, on_left, on_right, settings = NULL, .auto_select = FALSE,
      .dim_mode = 'keep', .artificial_field = .random_attr_name()) {
      # Validate left and right
      left = self
      private$validate_join_operand('left', left, on_left)
      private$validate_join_operand('right', right, on_right)
      
      # Assert left and right keys lengths match
      assert(length(on_left) == length(on_right),
        "ERROR: ArrayOp$join: on_left[%s field(s)] and on_right[%s field(s)] must have the same length.",
        length(on_left), length(on_right))
      
      # Validate settings
      if(.has_len(settings)){
        settingKeys <- names(settings)
        if(!.has_len(settingKeys) || any(settingKeys == '')){
          stop("ERROR: ArrayOp$join: Settings must be a named list and each setting item must have a non-empty name when creating a JoinOp")
        }
      }
      # Validate selected fields
      hasSelected = .has_len(left$selected) || .has_len(right$selected)
      if(hasSelected && !.has_len(left$selected))
        assert(right$selected != on_right, 
          "ERROR: ArrayOp$join: Right operand's selected field(s) '%s' cannot be its join key(s) '%s' when there is no left operand fields selected.
Please select on left operand's fields OR do not select on either operand. Look into 'equi_join' documentation for more details.",
          paste(right$selected, collapse = ','), paste(on_right, collapse = ','))
      
      # Create setting items
      
      mergedSettings <- c(list(left_names = on_left, right_names = on_right), settings)
      # Values of setting items cannot be quoted.
      # But the whole 'key=value' needs single quotation according to equi_join plugin specs
      settingItems = mapply(private$to_equi_join_setting_item_str, names(mergedSettings), mergedSettings)

      keep_dimensions = (function(){
        val = settings[['keep_dimensions']]
        .has_len(val) && val == 1
      })()
      
      # Join two operands
      joinExpr <- sprintf(private$equi_join_template(),
        left$.to_join_operand_afl(on_left, keep_dimensions = keep_dimensions, artificial_field = .artificial_field), 
        right$.to_join_operand_afl(on_right, keep_dimensions = keep_dimensions, artificial_field = .artificial_field),
        paste(settingItems, collapse = ', '))
      
      
      dims = list(instance_id = 'int64', value_no = 'int64')
      attrs = (function() {
        if(hasSelected){
          rightSelected = right$selected %-% on_right  # Right key(s) are ignored/masked in equi_join
          as.character(unique(c(left$selected, rightSelected)))
        }
        else{
          leftRetained = if(keep_dimensions) left$attrs_n_dims else left$attrs
          rightRetained = if(keep_dimensions) right$attrs_n_dims else right$attrs
          return(c(on_left,
            leftRetained %-% on_left,
            rightRetained %-% on_right
          ))
        }
      }) ()
      dtypes = plyr::compact(c(dims, c(left$dtypes, right$dtypes)[attrs]))
      selectedFields = if(hasSelected) attrs else NULL
      joinedOp = self$create_new(joinExpr, names(dims), attrs, dtypes = dtypes)
      if(hasSelected) {
        joinedOp = joinedOp$reshape(select = selectedFields, dim_mode = .dim_mode, artificial_field = .artificial_field)
        if(.auto_select)
          joinedOp = joinedOp$select(selectedFields)
      }
      joinedOp
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance by loading a file and checking it against an ArrayOp template (self).
    #'
    #' The ArrayOp instance where this function is called from serves as a template. By defulat, it assumes file 
    #' column headers match the template's dims and attrs; otherwise an explicit file_headers can be provided and will 
    #' be used to match the template's schema. 
    #' @param filepath A single file path
    #' @param aio_settings Customized settings of aio_input
    #' @param field_conversion If NULL (default), use template's field type to convert aio_input attributes; Or provide
    #' a list for customized field conversion
    #' @param file_headers Column headers of the input file regardless of whether there is a header line in the file.
    #' Default NULL assumes file headers match self$dims_n_attrs. If the headers order are different or there are 
    #' columns to skip, please provide a string vector, in which case only columns with matching template field are 
    #' loaded. Names of the unmatching column headers are irrelevant. 
    #'
    #' @return A new ArrayOp instance with matching fields
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
      settingItems = mapply(private$to_aio_setting_item_str, names(aio_settings), aio_settings)
      
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
      # return(self$create_new(projectedExpr, metaList = list()))
      return(self$create_new(projectedExpr, c(), names(fieldTypes), dtypes = fieldTypes))
    }
    ,
    #' @description 
    #' Create a new ArrayOp by matching a template against a source (self). 
    #' 
    #' The result has the same schema with the source.
    #' All fields in the template are compared to their matching source fields by equality, except for thos in 
    #' lower_bound/upper_bound which will be used as a range `[lower_bound, upper_bound]`.
    #' @param template A data.frame or ArrayOp used to reduce the number of source cells without changing its schema
    #' @param op_mode ['filter', 'cross_between']
    #' @param lower_bound Field names as lower bounds. 
    #' @param upper_bound Field names as upper bounds.
    #' @param field_mapping A named list where name is source field name and value is template field name.
    #' Default NULL: fields are mapped between template and source by field names only. 
    #' If there is mapping fields in the template which are intended for lower or upper bound, 
    #' provide an empty list or a list with matching fields 
    #' @return A new ArrayOp instance which has the same schema as the source. 
    match = function(template, op_mode, lower_bound = NULL, upper_bound = NULL, field_mapping = NULL, ...){
      assert_not_has_len(names(lower_bound) %n% names(field_mapping), 
        "ERROR: ArrayOp$match: Field names in param 'lower_bound' and 'field_mapping' cannot overlap: '%s'",
        paste(names(lower_bound) %n% names(field_mapping), collapse = ','))
      assert_not_has_len(names(upper_bound) %n% names(field_mapping), 
        "ERROR: ArrayOp$match: Field names in param 'upper_bound' and 'field_mapping' cannot overlap: '%s'",
        paste(names(upper_bound) %n% names(field_mapping), collapse = ','))
      if(.has_len(lower_bound))
        assert_named_list(lower_bound, "ERROR: ArrayOp$match: lower_bound if provided must be a named list.")
      if(.has_len(upper_bound))
        assert_named_list(upper_bound, "ERROR: ArrayOp$match: upper_bound if provided must be a named list.")
      
      filter_mode = function(...){
        assert(inherits(template, 'data.frame'), 
          "ERROR: ArrayOp$match: filter mode: template must be a data.frame, but got: %s", class(template))
        unmatchedCols = names(template) %-% self$dims_n_attrs %-% lower_bound %-% upper_bound
        assert_not_has_len(unmatchedCols, 
          "ERROR: ArrayOp$match: filter mode: template field(s) not matching the source: '%s'",
          paste(unmatchedCols, collapse = ','))
        
        colTypes = sapply(template, class)
        needQuotes = colTypes != 'numeric'
        valueStrTemplates = lapply(needQuotes, .ifelse, "'%s'", "%s")
        # Iterate on the template data frame
        convertRow = function(eachRow, colNames) {
          rowValues = mapply(sprintf, valueStrTemplates, eachRow)
          # Each filter item per row per field
          rowItems = mapply(function(name, val){
            if(name %in% lower_bound) {
              operator = '>='
              name = names(lower_bound)[lower_bound == name][[1]]
            }
            else if(name %in% upper_bound) {
              operator = '<='
              name = names(upper_bound)[upper_bound == name][[1]]
            }
            else operator = '='
            sprintf("%s%s%s", name, operator, val)
          }, colNames, rowValues)
          sprintf(
            .ifelse(length(rowValues) > 1, "(%s)", "%s"), # String template for a row
            paste(rowItems, collapse = ' and ')
          )
        }
        
        filter_afl = paste( apply(template, 1, convertRow, names(template)), collapse = ' or ' )
        return(afl(self %filter% filter_afl))
      }
      
      cross_between_mode = function(...){
        assert(inherits(template, 'ArrayOpBase'), 
          "ERROR: ArrayOp$match: cross_between mode: template must be a ArrayOp instance, but got: %s", class(template))
        if(is.null(field_mapping)){
          dimMatchMarks = self$dims %in% template$dims_n_attrs
          matchedDims = template$dims_n_attrs %n% self$dims
          field_mapping = as.list(structure(matchedDims, names = matchedDims))
        }
        else {
          matchedDims = names(field_mapping) %n% self$dims
          dimMatchMarks = self$dims %in% names(field_mapping)
        }
        
        assert_has_len(matchedDims %u% names(lower_bound) %u% names(upper_bound),
          "ERROR: ArrayOp$match: cross_between mode: none of the template fields '%s' matches the source's dimensions: '%s'.
Only dimensions are matched in this mode. Attributes are ignored even if they are provided.",
          paste(template$dims_n_attrs, collapse = ','), paste(self$dims, collapse = ','))
        
        # get region array's attr values
        getRegionArrayAttrValue = function(default, low){
          res = rep(default, length(self$dims))
          for(i in 1:length(self$dims)){
            mainDimKeyName = self$dims[[i]]
            if(low && mainDimKeyName %in% names(lower_bound)){
              res[[i]] <- lower_bound[[mainDimKeyName]]
            }
            else if(!low && mainDimKeyName %in% names(upper_bound)){
              res[[i]] <- upper_bound[[mainDimKeyName]]
            }
            else if(dimMatchMarks[[i]]){
              res[[i]] <- sprintf("int64(%s)", field_mapping[[mainDimKeyName]])
            } 
          }
          return(res)
        }
        
        regionLowAttrValues = getRegionArrayAttrValue(MIN_DIM, low = TRUE)
        regionHighAttrValues = getRegionArrayAttrValue(MAX_DIM, low = FALSE)
        
        regionLowAttrNames = sprintf('_%s_low', self$dims)
        regionHighAttrNames = sprintf('_%s_high', self$dims)
        
        # apply new attributes as the region array in 'cross_between'
        applyExpr = afl_join_fields(regionLowAttrNames, regionLowAttrValues, regionHighAttrNames, regionHighAttrValues)
        afl_literal = afl(
          self %cross_between%
            afl(template %apply% applyExpr %project%
                c(regionLowAttrNames, regionHighAttrNames))
        )
        return(afl_literal)
      }
        
      aflExpr = switch(op_mode,
        'filter' = filter_mode,
        'cross_between' = cross_between_mode,
        stopf("ERROR: ArrayOp$match: unknown op_mode '%s'.", op_mode)
      )(...)
      self$create_new_with_same_schema(aflExpr)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance from 'build'ing a data.frame
    #' 
    #' All matching fields are built as attributes of the result ArrayOp.
    #' @param df a data.frame, where all column names must all validate template fields.
    #' @param artificial_field A field name used as the artificial dimension name in `build` scidb operator
    #' By default, a random string is generated, and the dimension starts from 0. 
    #' A customized dimension can be provided e.g. 'z=42:*' or 'z=0:*:0:1000'.
    #' @return A new ArrayOp instance whose attributes share the same name and data types with the template's fields.
    build_new = function(df,  artificial_field = .random_attr_name()) {
      assert(inherits(df, c('data.frame')), "ERROR: ArrayOp$build_new: unknown df class '%s'. 
Only data.frame is supported", class(df))
      
      builtAttrs = names(df)
      
      dfNonMatchingCols = builtAttrs %-% self$dims_n_attrs
      assert_not_has_len(dfNonMatchingCols, "ERROR: ArrayOp$build_new: df column(s) '%s' not found in template %s",
        paste(dfNonMatchingCols, collapse = ','), self$to_afl())
      
      builtDtypes = self$get_field_types(builtAttrs)
      # Convert a single value to its proper string representation in `build` expressions.
      stringify_in_build = function(single_value) {
        if(is.na(single_value) || is.null(single_value))
          "" # Return an empty string for NA or NULL
        else if(is.character(single_value) || is.factor(single_value))
          sprintf("\\'%s\\'", single_value)  # String literals
        else if(is.logical(single_value) && single_value) "true"
        else if(is.logical(single_value) && !single_value) "false"
        else sprintf("%s", single_value)  # Other types
      }
     
      #' Create a build expression from a data.frame and specified scidb data types
      attrStr = paste(builtAttrs, builtDtypes, collapse = ',', sep = ':')
      rowStrs = by(df, 1:nrow(df), function(row){
        sprintf("(%s)", paste(lapply(row[1,], stringify_in_build), collapse = ','))
      })
      afl_literal = sprintf("build(<%s>[%s], '[%s]', true)", 
        attrStr, artificial_field, paste(rowStrs, collapse = ','))
      
      return(self$create_new(afl_literal, artificial_field, builtAttrs, 
        dtypes = c(list(artificial_field = 'int64'), builtDtypes)))
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance that with added auto incremented fields
    #' 
    #' @param reference ArrayOp instance to draw existing max id from 
    #' @param source_field 
    #' @param ref_field 
    #' @param source_start 
    #' @param ref_start 
    #' @param new_field 
    set_auto_increment_field = function(reference, source_field, ref_field, source_start, ref_start, new_field = NULL) {
      assert(inherits(reference, 'ArrayOpBase'),
        "ERROR: ArrayOp$set_auto_increment_field: param 'reference' must be ArrayOp, but got '%s' instead.", 
        class(reference))
      
      assert(is.character(source_field), 
        "ERROR: ArrayOp$set_auto_increment_field: param 'source_field' must be a character, but got '%s' instead.", 
        class(source_field))
      assert(is.character(ref_field), 
        "ERROR: ArrayOp$set_auto_increment_field: param 'ref_field' must be a character, but got '%s' instead.", 
        class(ref_field))
      
      assert(is.numeric(source_start), 
        "ERROR: ArrayOp$set_auto_increment_field: param 'source_start' must be a numeric, but got '%s' instead.", 
        class(source_start))
      assert(is.numeric(ref_start), 
        "ERROR: ArrayOp$set_auto_increment_field: param 'ref_start' must be a numeric, but got '%s' instead.", 
        class(ref_start))
      
      assert_not_has_len(source_field %-% self$dims_n_attrs, 
        "ERROR: ArrayOp$set_auto_increment_field: source_field '%s' not exist.",
        paste(source_field %-% self$dims_n_attrs, ','))
      assert_not_has_len(ref_field %-% reference$dims_n_attrs, 
        "ERROR: ArrayOp$set_auto_increment_field: ref_field '%s' not exist.",
        paste(ref_field %-% ref$dims_n_attrs, ','))
      
      refDims = ref_field %-% reference$attrs
      maxRefFields = sprintf("_max_%s", ref_field)
      aggFields = sprintf("max(%s) as %s", ref_field, maxRefFields)
      defaultOffset = ref_start - source_start
      nonDefaultOffset = 1 - source_start
        
      if(!.has_len(new_field)) new_field = ref_field
      newFieldExpr = sprintf("iif(%s is null, %s%s, %s+%s%s)", maxRefFields, 
        source_field, .to_signed_integer_str(defaultOffset), 
        maxRefFields, source_field, .to_signed_integer_str(nonDefaultOffset))
      
      forAggregate = if(.has_len(refDims)) afl(reference %apply% afl_join_fields(refDims, refDims)) else reference
      aggregated = afl(forAggregate %aggregate% aggFields)
      crossJoined = afl(
        self 
        %cross_join% aggregated 
        %apply% 
          afl_join_fields(new_field, newFieldExpr)
      )
      self$spawn(added = new_field, dtypes = as.list(rlang::rep_named(new_field, 'int64')))$
        create_new_with_same_schema(crossJoined)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance that represents a writing data operation
    #' 
    #' If the dimension count, attribute count and data types match between the source(self) and target, 
    #' then no redimension will be performed, otherwise redimension on the source first.
    #'
    #' Redimension mode requires all target fields exist on the source disregard of being attributes or dimensions.
    #' Redimension mode does not check on whether source data types match the target because auto data conversion 
    #' occurs within scidb where necessary/applicable. 
    #' @param target A target ArrayOp the source data is written to. 
    #' @param append Append to existing target array if set to TRUE (default). 
    #' Otherwise replace the whole target array with the source.
    #' @param force_redimension Redimension the source even if the source fields match perfectly the target fields
    #' @param source_auto_increment a named number vector e.g. c(z=0), where the name is a source field and value is the starting index
    #' @param target_auto_increment a named number vector e.g. c(aid=0), where the name is a target field and value is the starting index.
    #' Here the `target_auto_increment` param only affects the initial load when the field is still null in the target array.
    #' @param anti_collision_field a target dimension name which exsits only to resolve cell collision 
    #' (ie. cells with the same dimension coordinate).
    write_to = function(target, append = TRUE, force_redimension = FALSE, 
      source_auto_increment = NULL, target_auto_increment = NULL, 
      anti_collision_field = NULL) {
      assert(inherits(target, 'ArrayOpBase'),
        "ERROR: ArrayOp$write_to: param target must be ArrayOp, but got '%s' instead.", class(target))
      
      # If exact field dtype match
      exactDtypeMatch = 
        length(self$dims) == length(target$dims) &&
        length(self$attrs) == length(target$attrs) && {
          srcDtypes = self$get_field_types(self$dims_n_attrs)
          targetDtypes = target$get_field_types(target$dims_n_attrs)
          length(srcDtypes) == length(targetDtypes) && 
            all(as.character(srcDtypes) == as.character(targetDtypes))
        }
      
      src = self
      if(!exactDtypeMatch || force_redimension){
        # If data types do not exactly match, redimension is required
        missingFields = target$dims_n_attrs %-% self$dims_n_attrs %-% names(target_auto_increment) %-% anti_collision_field
        # If no auto increment set, then all target fields should be matched
        assert_not_has_len(missingFields, 
                           "ERROR: ArrayOp$write_to: redimension mode: Source field(s) '%s' not found in Target", 
                           paste(missingFields, collapse = ','))
        
        # If there is an auto-incremnt field, it needs to be inferred from the params
        # After this step, 'src' is an updated ArrayOp with auto-incremented id calculated (during AFL execution only due to lazy evaluation)
        if(.has_len(target_auto_increment)){
          src = self$set_auto_increment_field(target, 
            source_field = names(source_auto_increment), ref_field = names(target_auto_increment), 
            source_start = source_auto_increment, ref_start = target_auto_increment)
        }
        
        # If there is anti_collision_field, more actions are required to avoid cell collision
        # After this step, 'src' is an updated ArrayOp with auto_collision_field set properly
        if(.has_len(anti_collision_field)){
          assert(is.character(anti_collision_field) && length(anti_collision_field) == 1, 
                 "ERROR: ArrayOp$write_to: redimension mode: param 'anti_collision_field' if provided must be a single string, but got: %s", anti_collision_field)
          
          # Target dimensions other than the anti_collision_field
          regularTargetDims = target$dims %-% anti_collision_field 
          
          # Redimension source with a different field name for the anti-collision-field
          srcAltId = sprintf("_src_%s", anti_collision_field)
          renamedList = as.list(structure(srcAltId, names = anti_collision_field))
          renamedTarget = target$spawn(renamed = renamedList)
          redimenedSource = renamedTarget$create_new_with_same_schema(afl(
            src %redimension% renamedTarget$to_schema_str() %apply% c(srcAltId, srcAltId)
          ))
          
          # Get the max anti-collision-field from group aggregating the target on the remainder of target dimensions
          targetAltIdMax = sprintf("_max_%s", anti_collision_field)
          groupedTarget = target$create_new_with_same_schema(afl(
            target %apply% c(anti_collision_field, anti_collision_field) %grouped_aggregate%
              c(sprintf("max(%s) as %s", anti_collision_field, targetAltIdMax), regularTargetDims)
          ))
          
          # Left join on the remainder of the target dimensions
          joined = redimenedSource$join(groupedTarget, on_left = regularTargetDims, on_right = regularTargetDims,
                                        settings = list(left_outer=1))
          
          # Finally calculate the values of anti_collision_field
          src = target$create_new_with_same_schema(afl(
            joined %apply% c(anti_collision_field, sprintf(
              "iif(%s is null, %s, %s + %s + 1)", targetAltIdMax, srcAltId, srcAltId, targetAltIdMax
            ))
          ))
        }
        
        src = afl(src %redimension% target)
      }
      
      afl_literal = if(append) afl(src %insert% target) else afl(src %store% target)
      return(self$create_new(afl_literal, c(), c()))
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance from a `self` template
    #' 
    #' This function is mainly for array schema string generation where we might want to rename and/or exclude certain
    #' fields of the template. Data types and dimension specs will be inherited from the template unless provided. 
    #' 
    #' Note: except for scidb build or aio_input operators, the spawned ArrayOp is not meaningful semantically. So do not
    #' use this function for operations other than 'build'/ArrayOp$build_new and 'aio_input'/ArrayOp$load_file
    spawn = function(renamed = NULL, added = NULL, excluded = NULL, dtypes = NULL, dim_specs = NULL) {
      if(.has_len(renamed)){
        assert_named_list(renamed, "ERROR: ArrayOp$spawn: 'renamed' param must be a named list, but got: %s", renamed)
      }
      assert(!.has_len(added) || is.character(added), 
        "ERROR: ArrayOp$spawn: 'added' param must be an R character, but got: %s", class(added))
      assert(!.has_len(excluded) || is.character(excluded), 
        "ERROR: ArrayOp$spawn: 'excluded' param must be an R character, but got: %s", class(excluded))
      
      attrs = self$attrs
      dims = self$dims
      oldDtypes = self$dtypes
      oldDimSpecs = self$get_dim_specs()
      
      # Rename the existing
      if(.has_len(renamed)){
        renamedOldFields = names(renamed)
        attrs = as.character(replace(attrs, attrs %in% renamedOldFields, plyr::compact(renamed[attrs])))
        dims = as.character(replace(dims, dims %in% renamedOldFields, plyr::compact(renamed[dims])))
        namesOldDtypes = names(oldDtypes)
        names(oldDtypes) <- replace(namesOldDtypes, namesOldDtypes %in% renamedOldFields, plyr::compact(renamed[namesOldDtypes]))
        namesOldDimSpecs = names(oldDimSpecs)
        names(oldDimSpecs) <- replace(namesOldDimSpecs, namesOldDimSpecs %in% renamedOldFields, plyr::compact(renamed[namesOldDimSpecs]))
      }
      
      # Add new fields
      if(.has_len(added)){
        addedDims = added %n% names(dim_specs)
        addedAttrs = added %-% names(dim_specs)
        attrs = attrs %u% addedAttrs
        dims = dims %u% addedDims
      }
      
      # Exclude the excluded
      if(.has_len(excluded)){
        attrs = attrs %-% excluded
        dims = dims %-% excluded
      }
      
      dtypes = .ifelse(.has_len(dtypes), c(dtypes, oldDtypes), oldDtypes)
      dtypes = dtypes[c(dims, attrs)]
      
      dim_specs = .ifelse(.has_len(dim_specs), c(dim_specs, oldDimSpecs), oldDimSpecs)
      
      self$create_new("Spawned ArrayOp", dims, attrs, dtypes, dim_specs = dim_specs)
    }

    # AFL -------------------------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' AFL representation of the ArrayOp instance
    #' 
    #' The ArrayOp instance may have 'selected' fields but they are not reflected in the result.
    #' 'selected' fields are meaningful in `to_df_afl` and `.to_afl_explicit` functions, where the parent operation
    #' treats dimension and attributes differently.
    #' 
    #' @return an AFL expression string
    to_afl = function() {
      return(private$raw_afl)
    }
    ,
    #' @description 
    #' Return AFL suitable for retrieving data.frame.
    #'
    #' scidb::iquery has a param `only_attributes`, which, if set TRUE, will effectively drop all dims.
    #' @param drop_dims Whether self's dimensions are dropped when generating AFL for data.frame conversion
    #' @return AFL string
    to_df_afl = function(drop_dims = FALSE, artificial_field = .random_attr_name()) {
      return(self$.to_afl_explicit(drop_dims, self$selected, artificial_field = artificial_field))
    }
    ,
    #' @description 
    #' Return a schema representation of the ArrayOp <attr1 [, attr2 ...]> \[dim1 [;dim2]\]
    #' 
    #' @return AFL string
    to_schema_str = function() {
      attrStr = paste(self$attrs, self$get_field_types(self$attrs), sep = ':', collapse = ',')
      dimStrItems = mapply(function(dimName, dimSpec){
        if(.has_len(dimSpec) && nchar(dimSpec) > 0) sprintf("%s=%s", dimName, dimSpec)
        else dimName
      }, self$dims, self$get_dim_specs())
      dimStr = paste(dimStrItems, collapse = ';')
      sprintf("<%s> [%s]", attrStr, dimStr)
    }
    
    # Old -------------------------------------------------------------------------------------------------------------
    ,

    #' @description 
    #' Returns AFL when self used as an operand in another parent operation.
    #'
    #' By default, 1. dimensions are not dropped in parent operation; 2. no intent to select fields
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' @param selected_fields which fields are selected no matter what the parent operation is.
    #' If NULL, self fields will pass on by default depending on the parent operation.
    .to_afl_explicit = function(drop_dims = FALSE, selected_fields = NULL, artificial_field = .random_attr_name()){

      # No intent to select fields
      if(!.has_len(selected_fields))
        return(self$to_afl())

      # 'selected_fields' is specified in sub-classes.
      # We need to ensure selected fields are passed on in parent operations.

      # dims are dropped in parent operations.
      if(drop_dims){
        selectedDims = base::intersect(selected_fields, self$dims)
        inner = if(.has_len(selectedDims))
          afl(self %apply% paste(selectedDims, selectedDims, sep = ',', collapse = ','))
          else self$to_afl()
        return(afl(inner %project% afl_join_fields(selected_fields)))
      }

      # drop_dims = F. All dims are preserved in parent operations, we can only select/drop attrs.
      selectedAttrs = base::intersect(selected_fields, self$attrs)
      if(.has_len(selectedAttrs))  # If some attributes selected
        return(afl(self %project% selectedAttrs))

      # If no attributes selected, we have to create an artificial attribute, then project it.
      # Because 'apply' doesn't work on array dimensions
      return(afl(self %apply% c(artificial_field, 'null')
            %project% artificial_field))
    }
    ,
    
    #' @description 
    #' Generate afl for ArrayOp used in a join context (equi_join).
    #'
    # Prerequisites include 1. dimensions/attributes/selected field names 2. .to_afl
    # Joined field(s) (attr or dimension) is converted to an attribute in equi_join result
    # All attributes and joined dimensions will be kept in equi_join result, which may not be optimal.
    # All non-joined dimensions will be dropped.
    # Detailed design and logic flow can be found at:
    # https://docs.google.com/spreadsheets/d/1kN7QgvQXXxcovW9q25d4TNb6tsf888-xhWdZW-WELWw/edit?usp=sharing
    #' @param keyFileds Field names as join keys
    #' @param keep_dimensiosn If `keep_dimensions` is specified in scidb `equi_join` operator
    .to_join_operand_afl = function(keyFields, keep_dimensions = FALSE, artificial_field = .random_attr_name()) {
      
      selectedFields = self$selected
      if(!.has_len(selectedFields))
        return(self$to_afl())
      
      dimensions = self$dims
      attributes = self$attrs
      arrName  = self$to_afl()
      
      applyList <- selectedFields %n% (dimensions %-% keyFields)
      projectList <-  applyList %u% (attributes %n% selectedFields) %u% (keyFields %-% dimensions)
      specialList <- dimensions %n% keyFields   # for joined-on dimensions which we also we want to keep in result
      if(keep_dimensions){
        applyList = applyList %-% self$dims
        projectList = projectList %-% self$dims
        # specialList = specialList %-% self$dims
      } 
      # case-1: When no attrs projected && selected dimensions, we need to create an artificial attr to project on.
      if(.has_len(specialList) && !.has_len(projectList) && .has_len(selectedFields)){
        res = afl(arrName %apply% c(artificial_field, 'null') %project% artificial_field)
      }
      # case-2: Regular mode. Just 'apply'/'project' for dimensions/attributes when needed.
      else{
        applyExpr = if(.has_len(applyList))
          afl(arrName %apply% afl_join_fields(applyList, applyList))
        else arrName
        res =  if(.has_len(projectList))
          afl(applyExpr %project% projectList)
        else applyExpr
      }
      return(res)
    }
    ,
    #' @description 
    #' Set ArrayOp meta data directly
    .set_meta = function(key, value) private$set_meta(key, value)
    ,
    #' @description 
    #' Get ArrayOp meta data directly
    .get_meta = function(key) private$get_meta(key)
  )
)

