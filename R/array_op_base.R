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
ArrayOp <- R6::R6Class("ArrayOp",
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
        private$metaList = list(dims = dims, attrs = attrs, dtypes = dtypes, validate_fields = validate_fields, ...)
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
    new = function(...){
      classConstructor = get(class(self))$new
      classConstructor(...)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance of the same class
    #' 
    #' The new instance shares all meta data with the template
    duplicate_with_same_specs = function(new_afl) {
      self$new(new_afl, metaList = private$metaList)
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
      self$duplicate_with_same_specs(newRawAfl)
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
      self$new(private$raw_afl, metaList = newMeta)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with a different schema/shape
    #' 
    #' @param select a non-empty list, where named items are new derived attributes and
    #' unamed string values are existing dimensions/attributes.
    #' @param dtypes
    reshape = function(select, dtypes = NULL, dim_mode = 'keep', artificial_field = .random_attr_name()) {
      
      assert_has_len(select,
        "ERROR: ArrayOp$reshape: 'select' param must be a non-empty character list, but got: %s", select)
      
      existingFields = if(.has_len(names(select))) select[names(select) == ''] else {
        as.character(select)
      }
      
      selectFieldNames = .ifelse(!.has_len(names(select)), as.character(select),
        mapply(function(name, value){
          if(name == '') value else name
        }, names(select), select, USE.NAMES = F)
      )
      newFields = select[names(select) != '']
      newFieldNames = names(newFields)
      mergedDtypes = c(self$dtypes, dtypes)
      
      keep = function(){
        selectedOldAttrs = base::intersect(existingFields, self$attrs)
        attrs = c(selectedOldAttrs, newFieldNames)
        inner = if(.has_len(newFieldNames)) afl(self, afl_join_fields(newFieldNames, newFields)) else self$to_afl()
        
        newAfl = if(.has_len(attrs)) afl(inner %project% attrs) 
          else {
            attrs = artificial_field
            afl(inner %apply% c(artificial_field, 'null') %project% artificial_field)
          }
        self$new(newAfl, self$dims, attrs, mergedDtypes, validate_fields = private$get_meta('validate_fields'))
      }
      
      drop = function() {
        unpacked = afl(self %unpack% artificial_field)
        newAfl = if(.has_len(newFieldNames)) 
          afl(unpacked %apply% afl_join_fields(newFieldNames, newFields) %project% selectFieldNames) 
        else afl(unpacked %project% selectFieldNames)
        newDtypes = c(mergedDtypes[selectFieldNames], structure('int64', names = artificial_field))
        self$new(newAfl, artificial_field, selectFieldNames, newDtypes, validate_fields = private$get_meta('validate_fields'))
      }
      
      ignore_in_parent = function() {
        identity
      }
      
      switch (dim_mode,
        'keep' = keep,
        'drop' = drop,
        'ignore_in_parent' = ignore_in_parent,
        stopf("ERROR: ArrayOp$reshape: invalid 'dim_mode' %s.", dim_mode)
      ) ()
    }
    

    # AFL -------------------------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Return AFL when self used as an operand in another parent operation.
    #' 
    #' Implemented by calling to_afl_explicit with `selected_fields = self$selected`
    #'
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' By default, dimensions are not dropped in parent operation
    #' But in some operations, dimensions are dropped or converted to attributes
    #' e.g. equi_join creates two artificial dimensions and discard any existing dimensions of two operands.
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
    to_df_afl = function(drop_dims = FALSE, artificial_attr = .random_attr_name()) {
      return(self$to_afl(drop_dims, artificial_attr = artificial_attr))
    }
    
    # Old -------------------------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Functions below translate array operations into SciDb AFL expressions/statements.
    #'
    #' NOTE: this method do not take into acount field selection
    #' Sub-classes should override this function.
    #' Doing so equips sub-classes with sensible behavior defined below.
    #' @return  AFL when self used as an operand in another parent operation without considering fields selection
    .raw_afl = function() {
      private$raw_afl
    },

    #' @description 
    #' Returns AFL when self used as an operand in another parent operation.
    #'
    #' By default, 1. dimensions are not dropped in parent operation; 2. no intent to select fields
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' @param selected_fields which fields are selected no matter what the parent operation is.
    #' If NULL, self fields will pass on by default depending on the parent operation.
    .to_afl_explicit = function(drop_dims = FALSE, selected_fields = NULL, artificial_attr = .random_attr_name()){

      # No intent to select fields
      if(!.has_len(selected_fields))
        return(private$raw_afl)

      # 'selected_fields' is specified in sub-classes.
      # We need to ensure selected fields are passed on in parent operations.

      # dims are dropped in parent operations.
      if(drop_dims){
        selectedDims = base::intersect(selected_fields, self$dims)
        inner = if(.has_len(selectedDims))
          afl(self$.raw_afl() %apply% paste(selectedDims, selectedDims, sep = ',', collapse = ','))
          else self$.raw_afl()
        return(afl(inner %project% afl_join_fields(selected_fields)))
      }

      # drop_dims = F. All dims are preserved in parent operations, we can only select/drop attrs.
      selectedAttrs = base::intersect(selected_fields, self$attrs)
      if(.has_len(selectedAttrs))  # If some attributes selected
        return(afl(self$.raw_afl() %project% selectedAttrs))

      # If no attributes selected, we have to create an artificial attribute, then project it.
      # Because 'apply' doesn't work on array dimensions
      return(afl(self$.raw_afl() %apply% c(artificial_attr, 'null')
            %project% artificial_attr))
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
    to_join_operand_afl = function(keyFields, keep_dimensions = FALSE, artificial_attr = .random_attr_name()) {

      selectedFields = self$selected

      dimensions = self$dims
      attributes = self$attrs
      arrName  = self$.raw_afl()

      applyList <-base::intersect(selectedFields, base::setdiff(dimensions, keyFields))
      projectList <- base::union(applyList, base::intersect(attributes, selectedFields))
      specialList <- base::intersect(dimensions, keyFields)   # for joined-on dimensions which we also we want to keep in result

      # case-1: When no attrs projected && selected dimensions, we need to create an artificial attr to project on.
      if(.has_len(specialList) && !.has_len(projectList) && .has_len(selectedFields)){
        res = afl(arrName %apply% c(artificial_attr, 'null') %project% artificial_attr)
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
    # Convenience methods which return CustomizedOp and simplify AFL generation ---------------------------------------
    ,
    #' @description 
    #' Create a CustomizedOp with a changed schema. Implemented with scidb `unpack` operator
    #' 
    #' **NOTE:**Allow adding new fields
    #' @param fields A non-empty list where named items are new attributes and
    #' string values without names are existing dim/attribute
    #' @param dtypes A named list of fieldName:fieldType for newly create fields
    #' @param unpack_dim_name Customized dimension name in scidb `unpack` operator.
    #' @return A CustomizedOp instance
    transform_unpack = function(fields, dtypes = list(), unpack_dim_name = 'z') {
      
      assert(is.list(fields) && .has_len(fields), "ArrayOp$transform 'fields' arg must be a non-empty list")
      assert(!.has_len(dtypes) || all(names(dtypes) != ''),
        "ArrayOp$transform 'dytypes' arg, if provided, must be a named list, i.e. list(newFieldName='newFieldType',...)")
      
      rawAfl = afl(self %unpack% unpack_dim_name)
      
      newFields = fields[names(fields) != '']
      if(.has_len(newFields)){
        rawAfl = afl(rawAfl %apply% afl_join_fields(names(newFields), newFields))
      }
      
      projectedFieldNames = .ifelse(is.null(names(fields)), as.character(fields),
        mapply(function(name, value){
          if(name == '') value else name
        }, names(fields), fields, USE.NAMES = F)
      )
      rawAfl = afl(rawAfl %project% projectedFieldNames)
      return(CustomizedOp$new(rawAfl, dims = unpack_dim_name, attrs = projectedFieldNames
        ,field_types = c(self$get_field_types(projectedFieldNames), dtypes)
        ,validate_fields = TRUE)
      )
    }
    ,
    #' @description 
    #' Convert a data.frame to a CustomizedOp that matches self's fields
    #' @param mode 'build' or 'upload'.
    #' 'build' mode is implemented with scidb `build` operator;
    #' 'upload' mode first upload the data.frame as an array, then add missing fields if needed.
    #' @param operand a data.frame
    #' @param missing_fields A named list specifying missing field expressions
    #' @param build_dimension A string used as the dimension name of `build` operation. Only applicable in 'build' mode.
    #' @return A CustomizedOp that can be used in WriteOp
    convert_df = function(operand, mode, missing_values = list(), validate_fields = TRUE, build_dimension = 'j') {
      
      assert(inherits(operand, c('data.frame')), "build_df: unknown operand class '%s'", class(operand))
      assert(mode %in% c('build', 'upload'), "build_df: unknown mode: %s", mode)
      
      .get_df_build = function(df, dtypes, dim){
        #' Create a build expression from a data.frame and specified scidb data types
        
        attrStr = paste(names(df), dtypes, collapse = ',', sep = ':')
        
        # Generate cell strings
        rowStrs = by(df, 1:nrow(df), function(row){
          sprintf("(%s)", paste(lapply(row[1,], .stringify_in_build), collapse = ','))
        })
        
        result = sprintf("build(<%s>[%s], '[%s]', true)", attrStr, dim, paste(rowStrs, collapse = ','))
        result
      }
      
      .stringify_in_build = function(single_value) {
        #' Convert a single value to its proper string representation in `build` expressions.
        if(is.na(single_value) || is.null(single_value))
          "" # Return an empty string for NA or NULL
        else if(is.character(single_value) || is.factor(single_value))
          sprintf("\\'%s\\'", single_value)  # String literals
        else sprintf("%s", single_value)  # Other types
      }
      
      template = self
      uploaded = NULL
      afl = if(mode == 'build'){
        dfNonMatchingCols = base::setdiff(names(operand), template$dims_n_attrs)
        assert(!.has_len(dfNonMatchingCols), "data.frame column(s) '%s' not found in template %s",
          paste(dfNonMatchingCols, collapse = ','), template$to_afl())
        .get_df_build(operand, template$get_field_types(names(operand)), dim = build_dimension)
      } else if(mode == 'upload') {
        uploaded = DEP$df_to_arrayop_func(operand)
        uploaded$to_afl()
      }
      
      # fields missing in the data.frame columns but present in template's fields
      missingTemplateFields = base::setdiff(template$dims_n_attrs, names(operand))
      if(.has_len(missingTemplateFields)){
        # If validate_fields, ensure missing_values matches missing fields
        if(validate_fields)
          assert(base::setequal(names(missing_values), missingTemplateFields),
            "Missing values '%s' NOT matching missing template fields '%s'",
            paste(names(missing_values), collapse = ','), paste(missingTemplateFields, collapse = ','))
        
        if(.has_len(missing_values))
          afl = afl(afl %apply% afl_join_fields(names(missing_values), missing_values))
      }
      # Add refs to 'uploaded' (if exists) to avoid garbage collection
      return(CustomizedOp$new(afl, refs = uploaded))
    }
    ,
    #' @description 
    #' Create a new CustomizedOp using the same schema data type but renamed fields (ie. dimensions and/or attributes)
    #' @param name_list A named list. list(old_field_name = 'new_field_name', ...)
    #' @return A Customized instance
    rename_fields = function(name_list){
      
      assert(is.list(name_list) && !is.null(names(name_list)) && all(names(name_list) != ''),
        "rename_fields: arg 'name_list' must be a named list where names are old field names, values are new ones.")
      
      .replace = function(strings, changeList) {
        changeList = plyr::compact(changeList[strings])
        namedStringVector = structure(strings, names = strings)
        changed = replace(namedStringVector, names(changeList), changeList)
        return(as.character(changed))
      }
      
      dims = self$dims
      attrs = self$attrs
      dtypes = self$get_field_types(self$dims_n_attrs)
      
      customizedOp = CustomizedOp$new(self$.raw_afl(), .replace(dims, name_list), .replace(attrs, name_list),
        field_types = structure(dtypes, names = .replace(names(dtypes), name_list)) )
      return(customizedOp)
    }
  )
)

