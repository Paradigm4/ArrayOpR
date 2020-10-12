# A base class for other specialized array operands/operations, e.g. SubsetOp, JoinOp, ArraySchema

MAX_DIM = '4611686018427387903'
MIN_DIM = '-4611686018427387902'

#' A string representation of ArrayOp instance
#' 
#' @export
str.ArrayOpBase = function(op) {
  sprintf("%s %s", op$to_afl(), op$to_schema_str())
}

#' A string representation of ArrayOp instance
#' 
#' @export
print.ArrayOpBase = function(op) {
  sprintf("%s %s", op$to_afl(), op$to_schema_str())
}

#' ArrayOp base class that encapsulates a scidb array or array operations
#' 
#' @description 
#' ArrayOp class instances denote scidb array operations and operands, hence the name. 
#' 
#' Operands can be plain scidb array names or (potentially nested) operations on arrays.
#' 
#' Most ArrayOp class methods return a new ArrayOp instance and the original 
#' instance on which methods are invoked from remains the same, 
#' i.e. ArrayOp instances are immutable.
#' 
#' @details 
#' One ArrayOp operation may involve one or multiple scidb operators and any number of operands.
#' in another operation. Operands and Opreration results can all be denoted by ArrayOp.
#' 
#' Sub-classes of ArrayOpBase deal with any syntax or operator changes in different 
#' SciDB version so that the ArrayOpBase class can provide a unified API on all 
#' supported SciDB versions. Currently SciDB V18 and V19 are supported.
#' 
#' Users of `arrayop` package shouldn't be concerned with a specific sub-class since
#' the `ScidbConnection` object automatically chooses the correct class version
#' and creates instances based on the scidb version it connects to.
#' 
#' Get arrayOp instances from the default ScidbConnection object.
#' See `arrayop::get_default_connection` for details.
ArrayOpBase <- R6::R6Class(
  "ArrayOpBase",
  cloneable = FALSE,                         
  # ArrayOpBase class internally maintains three private fields to represent its state.
  private = list(
    # Raw AFL encapsulated in the ArrayOp instance. 
    # A string of either an array name or potentially nested AFL operations.
    raw_afl = NULL
    ,
    # A list that stores other meta including: field types, dimension specs.
    metaList = NULL
    ,
    # A ScidbConnection instance which determines where and how AFL is executed.
    conn = NULL 
    ,
    # Generailized funtion to set or update a metadata with a (key, value) pair.
    set_meta = function(key, value) {
      private$metaList[[key]] <- value
    }
    ,
    # Generalized function to return a medatadata with key. 
    # Return NULL if key doesn't exist.
    get_meta = function(key) {
      private$metaList[[key]]
    }
    ,
    # todo: move connection setup to initialize function
    set_conn = function(connection) {
      private$conn = connection
    }
    ,
    # Hold references to the provided objects
    # Held obj refs accumulate
    # This is necessary to prevent R GC remove scidbr loaded arrays
    hold_refs = function(objs){
      key = ".refs"
      # if get_meta(key) is NULL, c(NULL, objs) still works
      private$set_meta(key, c(private$get_meta(key), objs))
    }
    ,
    # set the metadata to indicate the schema from this array_op instance is 
    # returned from SciDB rather than locally inferred in R
    confirm_schema_synced = function(){
      private$set_meta('is_schema_from_scidb', TRUE)
      invisible(self)
    }
    ,
    # @description 
    # Return ArrayOp field types as a list
    #
    # @param field_names R character. If NULL, defaults to `self$dims_n_attrs`, ie. dimensions and attributes.
    # @param .raw Default FALSE, full data types are returned; if set TRUE, only the raw data types are returned 
    # (raw data types are string, int32, int64, bool, etc, without scidb attribute specs such as: string compression 'zlib')
    # @return a named list where keys are field names, 
    # and values are scidb data types (single word or multiple words).
    # 
    # Throw an error if field_names are not all valid.
    get_field_types = function(field_names = NULL, .raw = FALSE){
      # field_names default to dims + attrs
      field_names = field_names %?% self$dims_n_attrs
      assert_empty(field_names %-% self$dims_n_attrs, "Invalid field(s): [{.value}]")
      result = new_named_list(self$dtypes[field_names], field_names)
      if(.raw){
        result = new_named_list(regmatches(result, regexpr("^\\w+", result)), names = field_names)
      }
      return(result)
    }
    ,
    # @description 
    # Get dimension specifications as a list
    # 
    # A dimension spec str is formatted as "lower bound : upper bound : overlap : chunk_length", 
    # as seen in scidb `show(array_name)` operator.
    # All dimensions' data types are int64, as returned by `get_field_types` function.
    # @param dim_names Default NULL equals all dimensions.
    # @return A named list where the name is dimension name and value is a dimension spec string
    # 
    # Throw an error if dim_names are not all valid dimension names.
    get_dim_specs = function(dim_names = NULL) {
      dim_names = dim_names %?% self$dims
      assert_empty(dim_names %-% self$dims, "Invalid dimension(s): [{.value}]")
      private$get_meta('dim_specs')[dim_names]
    }
    ,
    # if no fields selected of self, then select all fields
    # otherwise return unchanged self
    auto_select = function() {
      if(length(self$selected) == 0L) self$select(self$dims_n_attrs)
      else self
    }
    ,
    validate_join_operand = function(side, operand, keys){
      assert(inherits(operand, "ArrayOpBase"),
        "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
        side, 'ArrayOpBase', paste(class(operand), collapse=","))
      
      assert(is.character(keys) && .not_empty(keys),
        "Join params 'on_%s' must be a non-empty R character, but got '%s' instead", 
        side, paste(class(keys), collapse = ","))
      
      assert_empty(keys %-% operand$dims_n_attrs, 
                   sprintf("Invalid key field(s): [{.value}] on the %s operand", side))
    }
    ,
    validate_join_params = function(left, right, by.x, by.y, settings) {
      private$validate_join_operand('left', left, by.x)
      private$validate_join_operand('right', right, by.y)
      
      # Assert left and right keys lengths match
      assert(length(by.x) == length(by.y),
        "ArrayOp join: by.x[%s field(s)] and by.y[%s field(s)] must have the same length.",
        length(by.x), length(by.y))
      
      # Validate settings
      if(.not_empty(settings)){
        settingKeys <- names(settings)
        if(.is_empty(settingKeys) || any(settingKeys == '')){
          stop("ArrayOp join settings must be a named list and each setting item must have a non-empty name")
        }
      }
    }
    ,
    # @description 
    # Generate afl for ArrayOp used in a join context (equi_join).
    #
    #Prerequisites include 1. dimensions/attributes/selected field names 2. .to_afl
    #Joined field(s) (attr or dimension) is converted to an attribute in equi_join result
    #All attributes and joined dimensions will be kept in equi_join result, which may not be optimal.
    #All non-joined dimensions will be dropped.
    #Detailed design and logic flow can be found at:
    #https://docs.google.com/spreadsheets/d/1kN7QgvQXXxcovW9q25d4TNb6tsf888-xhWdZW-WELWw/edit?usp=sharing
    # @param keyFileds Field names as join keys
    # @param keep_dimensiosn If `keep_dimensions` is specified in scidb `equi_join` operator
    # @return An AFL string
    to_join_operand_afl = function(keyFields, keep_dimensions = FALSE, artificial_field = dbutils$random_field_name()) {
      
      selectedFields = self$selected
      if(.is_empty(selectedFields))
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
      if(.not_empty(specialList) && .is_empty(projectList) && .not_empty(selectedFields)){
        res = afl(arrName | apply(artificial_field, 'null') | project(artificial_field))
      }
      # case-2: Regular mode. Just 'apply'/'project' for dimensions/attributes when needed.
      else{
        applyExpr = if(.not_empty(applyList))
          afl(arrName | apply(aflutils$join_fields(applyList, applyList)))
        else arrName
        res =  if(.not_empty(projectList))
          afl(applyExpr | project(projectList))
        else applyExpr
      }
      return(res)
    }
    ,
    # @description 
    # Create a new ArrayOp instance by joining with another ArrayOp 
    # 
    # Currently implemented with scidb `equi_join` operator.
    # @param right The other ArrayOp instance to join with.
    # @param by.x R character vector. Join keys from the left (self).
    # @param by.y R character vector. Join keys from the `right`. Must be of the same length as `by.x`
    # @param by Join keys on both operand.
    # @param settings `equi_join` settings, a named list where both key and values are strings. 
    # @param .dim_mode How to reshape the resultant ArrayOp. Same meaning as in `ArrayOp$reshape` function. 
    # By default, dim_mode = 'keep', the artificial dimensions, namely `instance_id` and `value_no` from `equi_join`
    # are retained. If set to 'drop', the artificial dimensions will be removed. See `ArrayOp$reshape` for more details.
    # @param .artificial_field As in `ArrayOp$reshape`, it defaults to a random field name. It can be safely ignored in
    # client code. It exists only for test purposes. 
    # @return A new arrayOp 
    join = function(left, right, 
                    by.x = NULL, by.y = NULL, by = NULL, 
                    join_mode = 'equi_join',
                    settings = NULL, 
                    left_alias = '_L', right_alias = '_R') {
      if(.is_empty(by.x) && .is_empty(by.y) && .is_empty(by)){
        by = left$dims_n_attrs %n% right$dims_n_attrs
      }
      if(.not_empty(by)){
        by.x = by %u% by.x
        by.y = by %u% by.y
      }
      switch(join_mode,
             'equi_join' = private$equi_join,
             'cross_join' = private$cross_join,
             stopf("ERROR: ArrayOp$join: Invalid param 'join_mode' %s. Must be one of [equi_join, cross_join]", join_mode)
      )(
        left$.private$auto_select(), 
        right$.private$auto_select(), 
        by.x, by.y, settings = settings, 
        .left_alias = left_alias, .right_alias = right_alias)
    }
    ,
    # @description 
    # Disambiguate selected fields of join
    # 
    # If no name clashes between selected fields of both operands, 
    # nothing changes. 
    # Otherwise, add suffix to duplicated fields for each operand.
    # E.g. if a field named 'value' exists on both operands and is retained,
    # it will be renamed as 'value_left' and 'value_right' if left_alias and right_alias
    # are '_left' and '_right', respectively. 
    disambiguate_join_fields = function(left_fields, right_fields, .left_alias, .right_alias) {
      rawFields = c(left_fields, right_fields)
      duplicatedFields = rawFields[duplicated(rawFields)]
      if(length(duplicatedFields) > 0L){
        
        selectedLeftFields = Map(function(x) if(x %in% duplicatedFields) sprintf("%s.%s", .left_alias, x) else x, 
                                 left_fields)
        selectedRightFields = Map(function(x) if(x %in% duplicatedFields) sprintf("%s.%s", .right_alias, x) else x, 
                                  right_fields)
        selectedLeftNames= Map(function(x) if(x %in% duplicatedFields) sprintf("%s%s", x, .left_alias) else x, 
                               left_fields)
        selectedRightNames = Map(function(x) if(x %in% duplicatedFields) sprintf("%s%s", x, .right_alias) else x, 
                                 right_fields)
        .new_named_char_vec(
          values = selectedLeftFields %u% selectedRightFields,
          names = selectedLeftNames %u% selectedRightNames
        )
      } else { 
        .new_named_char_vec(rawFields)
      }
    }
    ,
    equi_join = function(left, right, by.x, by.y, settings = NULL,
      .left_alias = '_L', .right_alias = '_R') {
      # Validate join params
      private$validate_join_params(left, right, by.x, by.y, settings)
      
      # Validate selected fields
      hasSelected = .not_empty(left$selected) || .not_empty(right$selected)
      assertf(hasSelected, "There should be selected fields of equi_join operands.")
      
      # Create setting items
      mergedSettings <- c(list(left_names = by.x, right_names = by.y), settings)
      # Values of setting items cannot be quoted.
      # But the whole 'key=value' needs single quotation according to equi_join plugin specs
      settingItems = mapply(function(k, v) private$to_equi_join_setting_item_str(k, v, .left_alias, .right_alias), 
        names(mergedSettings), mergedSettings)

      keep_dimensions = .not_empty(settings[['keep_dimensions']]) && 
        (settings[['keep_dimensions']] == 1 || settings[['keep_dimensions']])
      
      # Join two operands
      joinExpr <- sprintf(private$equi_join_template(.left_alias, .right_alias),
        left$.private$to_join_operand_afl(by.x, keep_dimensions = keep_dimensions), 
        right$.private$to_join_operand_afl(by.y, keep_dimensions = keep_dimensions),
        paste(settingItems, collapse = ', '))
      
      
      dims = list(instance_id = 'int64', value_no = 'int64')
      attrs = as.character(unique(c(left$selected, right$selected %-% by.y)))
      dtypes = .remove_null_values(c(dims, c(left$dtypes, right$dtypes)[attrs]))
      
      selectedFields = private$disambiguate_join_fields(
        left$selected, right$selected %-% by.y,
        .left_alias, .right_alias
      )
      joinedOp = private$create_new(joinExpr, names(dims), attrs, dtypes = dtypes)
      return(
        # selectedFields names and values may be different due to disambiguation
        joinedOp$.private$reshape_fields(selectedFields)$select(names(selectedFields))
      )
    }
    ,
    cross_join = function(left, right, by.x, by.y, settings = NULL,  
      .left_alias = '_L', .right_alias = '_R') {
      # Scidb cross_join operator only allows join on operands' dimensions
      assert_keys_are_all_dimensions = function(side, operand, keys){
        nonDimKeys = keys %-% operand$dims
        assert_not_has_len(nonDimKeys, 
          "ERROR: ArrayOp$join: cross_join mode: All join keys must be dimensions, but %s key(s) '%s' are not.", 
          side, paste(nonDimKeys, collapse = ','))
      }
      # Validate join params
      # left = self
      private$validate_join_params(left, right, by.x, by.y, settings)
      assert_keys_are_all_dimensions('left', left, by.x)
      assert_keys_are_all_dimensions('right', right, by.y)
      # Construct the AFL
      joinDims = paste(sprintf(", %s.%s, %s.%s", .left_alias, by.x, .right_alias, by.y), collapse = '')
      aflStr = sprintf("cross_join(%s as %s, %s as %s %s)", 
                       # left$to_afl(), 
                       left$.private$to_join_operand_afl(by.x, keep_dimensions = TRUE), 
                       .left_alias,
                       # right$to_afl(),
                       right$.private$to_join_operand_afl(by.y, keep_dimensions = TRUE),
                       .right_alias, 
                       joinDims)
      attrs = c(left$attrs, right$attrs)
      dims = c(left$dims, right$dims %-% by.y)
      dtypes = c(left$.private$get_field_types(),
                 right$.private$get_field_types(right$dims_n_attrs %-% by.y))
      dim_specs = c(left$.private$get_dim_specs(), 
                    right$.private$get_dim_specs(right$dims %-% by.y))
      joinedOp = private$create_new(aflStr, dims = dims, attrs = attrs, dtypes = dtypes, dim_specs = dim_specs)
      
      selectedFields = private$disambiguate_join_fields(
        left$selected, right$selected %-% by.y,
        .left_alias, .right_alias
      )
      # cross_join keeps operands' dimensions, no need to apply dimensions as attributes
      selectedAttrs = Filter(function(x) !x %in% left$dims && !x %in% right$dims, selectedFields)
      joinedOp$.private$reshape_fields(selectedAttrs)$select(names(selectedFields))
    }
    ,
    # todo: Move logic from `match` method
    semi_join_by_df_filter = function(df, field_mapping, lower_bound, upper_bound){
      private$match(df, "filter", field_mapping = field_mapping, lower_bound = lower_bound, upper_bound = upper_bound)
    }
    ,
    # @description 
    # Create a new ArrayOp by matching a template against a source (self). 
    # 
    # The result has the same schema with the source.
    # All fields in the template are compared to their matching source fields by equality, except for thos in 
    # lower_bound/upper_bound which will be used as a range `[lower_bound, upper_bound]`.
    # @param template A data.frame or ArrayOp used to reduce the number of source cells without changing its schema
    # @param op_mode ['filter', 'cross_between']
    # @param lower_bound Field names as lower bounds. 
    # @param upper_bound Field names as upper bounds.
    # @param field_mapping A named list where name is source field name and value is template field name.
    # Default NULL: fields are mapped between template and source by field names only. 
    # If there is mapping fields in the template which are intended for lower or upper bound, 
    # provide an empty list or a list with matching fields 
    # @return A new ArrayOp instance which has the same schema as the source. 
    match = function(template, op_mode, lower_bound = NULL, upper_bound = NULL, field_mapping = NULL){
      assert_not_has_len(names(lower_bound) %n% names(field_mapping), 
                         "ERROR: ArrayOp$semi_join: Field names in param 'lower_bound' and 'field_mapping' cannot overlap: '%s'",
                         paste(names(lower_bound) %n% names(field_mapping), collapse = ','))
      assert_not_has_len(names(upper_bound) %n% names(field_mapping), 
                         "ERROR: ArrayOp$semi_join: Field names in param 'upper_bound' and 'field_mapping' cannot overlap: '%s'",
                         paste(names(upper_bound) %n% names(field_mapping), collapse = ','))
      if(.not_empty(lower_bound))
        assert_named_list(lower_bound, "ERROR: ArrayOp$semi_join: lower_bound if provided must be a named list.")
      if(.not_empty(upper_bound))
        assert_named_list(upper_bound, "ERROR: ArrayOp$semi_join: upper_bound if provided must be a named list.")
      
      filter_mode = function(){
        assert(inherits(template, 'data.frame'), 
               "ERROR: ArrayOp$semi_join: filter mode: template must be a data.frame, but got: %s", class(template))
        unmatchedCols = names(template) %-% self$dims_n_attrs %-% lower_bound %-% upper_bound
        assert_not_has_len(unmatchedCols, 
                           "ERROR: ArrayOp$semi_join: filter mode: template field(s) not matching the source: '%s'",
                           paste(unmatchedCols, collapse = ','))
        
        colTypes = sapply(template, class)
        needQuotes = !(colTypes %in% c('numeric', 'integer', 'integer64'))
        valueStrTemplates = lapply(needQuotes, .ifelse, "'%s'", "%s")
        # Iterate on the template data frame
        convertRow = function(eachRow, colNames) {
          rowValues = mapply(sprintf, valueStrTemplates, eachRow)
          # Each filter item per row per field
          rowItems = mapply(function(name, val){
            operator = as.character(NULL)
            sourceFieldName = as.character(NULL)
            if(name %in% lower_bound) {
              operator = c(operator, '>=')
              sourceFieldName = c(sourceFieldName, names(lower_bound)[lower_bound == name][[1]])
            }
            if(name %in% upper_bound) {
              operator = c(operator, '<=')
              sourceFieldName = c(sourceFieldName, names(upper_bound)[upper_bound == name][[1]])
            }
            if(!name %in% lower_bound && !name %in% upper_bound){
              operator = c(operator, '=')
              sourceFieldName = c(sourceFieldName, name)
            }
            sprintf("%s%s%s", sourceFieldName, operator, val)
          }, colNames, rowValues)
          if(is.list(rowItems))
            rowItems = do.call(c, rowItems) # in case of mixed vector and string
          sprintf(
            .ifelse(length(rowItems) > 1, "(%s)", "%s"), # String template for a row
            paste(rowItems, collapse = ' and ')
          )
        }
        
        filter_afl = paste( apply(template, 1, convertRow, names(template)), collapse = ' or ' )
        return(afl(self | filter(filter_afl)))
      }
      
      cross_between_mode = function(){
        assert(inherits(template, 'ArrayOpBase'), 
               "ERROR: ArrayOp$semi_join: cross_between mode: template must be a ArrayOp instance, but got: %s", class(template))
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
                       "ERROR: ArrayOp$semi_join: cross_between mode: none of the template fields '%s' matches the source's dimensions: '%s'.
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
        applyExpr = aflutils$join_fields(regionLowAttrNames, regionLowAttrValues, regionHighAttrNames, regionHighAttrValues)
        afl_literal = afl(
          self | cross_between(
            template | apply(applyExpr) | 
              project(regionLowAttrNames, regionHighAttrNames))
        )
        return(afl_literal)
      }
      
      index_lookup_mode = function() {
        assert(inherits(template, 'ArrayOpBase'), 
               "ERROR: ArrayOp$semi_join: index_lookup mode: template must be an ArrayOp instance, but got: [%s]", paste(class(template), collapse = ","))
        assert(length(template$attrs) == 1 && length(template$dims) == 1,
               "ERROR: ArrayOp$semi_join: index_lookup mode: template must have only one dimension and one attribute")
        if(is.null(field_mapping)){
          matchedFields = template$attrs %n% self$dims_n_attrs # find matched fields from template's attrs only (not in dims)
          assert(length(matchedFields) == 1, 
                 "ERROR: ArrayOp$semi_join: index_lookup mode: param 'field_mapping' == NULL, but template field '%s' does not match any source fields",
                 template$attrs)
          field_mapping = new_named_list(matchedFields, names = matchedFields)
        }
        assert(length(field_mapping) == 1, 
               "ERROR: ArrayOp$semi_join: index_lookup mode: there should be exactly one template attribute that matches source's fields, but %d field(s) found: %s",
               length(field_mapping), paste(field_mapping, collapse = ","))
        assert_not_has_len(lower_bound,
                           "ERROR: ArrayOp$semi_join: index_lookup mode: param 'lower_bound' is not allowed in this mode.")
        assert_not_has_len(upper_bound,
                           "ERROR: ArrayOp$semi_join: index_lookup mode: param 'upper_bound' is not allowed in this mode.")
        
        
        if(is.null(names(field_mapping))){ 
          # e.g. field_mapping = 'field_a' 
          sourceField = as.character(field_mapping)
          templateField = template$attrs
        } else {
          sourceField = names(field_mapping)
          templateField = as.character(field_mapping)
        }
        assert(sourceField %in% self$dims_n_attrs,
               "ERROR: ArrayOp$semi_join: index_lookup mode: '%s' is not a valid source field.", sourceField)
        assert(templateField %in% template$attrs,
               "ERROR: ArrayOp$semi_join: index_lookup mode: '%s' is not a valid template field.", templateField)
        
        isSourceAttrMatched = sourceField %in% self$attrs
        sourceMatchField = if(isSourceAttrMatched) sourceField else sprintf("attr_%s", sourceField)
        
        sourceOp = if(isSourceAttrMatched) self else {
          afl(self | apply(sourceMatchField, sourceField))
        }
        indexName = sprintf("index_%s", sourceField)
        templateOp = template
        if(templateField %in% self$dims_n_attrs) { 
          # if the template field name exist on the source too, there will be field name conflicts when we 'project'
          # templateOp = template$reshape(new_named_list(templateField, names = sprintf("alt_%s_", templateField)))
          templateOp = template$.private$reshape_fields(structure(templateField, names = sprintf("alt_%s_", templateField)))
        }
        
        afl(
          sourceOp | 
            index_lookup(templateOp, sourceMatchField, indexName) |
            filter(sprintf("%s is not null", indexName)) |
            project(self$attrs)
        )
      }
      
      # Select the mode function which returns an AFL statement
      aflExpr = switch(op_mode,
                       'filter' = filter_mode,
                       'cross_between' = cross_between_mode,
                       'index_lookup' = index_lookup_mode,
                       stopf("ERROR: ArrayOp$semi_join: unknown op_mode '%s'.", op_mode)
      )()
      self$spawn(aflExpr)
    }
    ,
    # @description 
    # Return a new arrayOp instance with the same version as `self`
    # 
    # Work in sub-class without requiring class names or constructor function.
    # @param ... The samme params with Repo$ArrayOp(...)
    # @return A new arrayOp 
    create_new = function(...){
      classConstructor = get(class(self))$new
      result = classConstructor(...)
      result$.private$set_conn(private$conn)
      result
    }
    ,
    # @description 
    # Create a new ArrayOp instance from 'build'ing a data.frame
    # 
    # All matching fields are built as attributes of the result ArrayOp.
    # Build operator accepts compound attribute types, so the result may have something like "build(<aa:string not null, ...)"
    # @param df a data.frame, where all column names must all validate template fields.
    # @param artificial_field A field name used as the artificial dimension name in `build` scidb operator
    # By default, a random string is generated, and the dimension starts from 0. 
    # A customized dimension can be provided e.g. `z=42:*` or `z=0:*:0:1000`.
    # @return A new ArrayOp instance whose attributes share the same name and data types with the template's fields.
    build_new = function(df, artificial_field = .random_attr_name(), as_scidb_data_frame = FALSE) {
      assert_inherits(df, "data.frame")
      
      builtAttrs = names(df)
      
      dfNonMatchingCols = builtAttrs %-% self$dims_n_attrs
      assert_not_has_len(dfNonMatchingCols, "ERROR: ArrayOp$build_new: df column(s) '%s' not found in template %s",
        paste(dfNonMatchingCols, collapse = ','), self$to_afl())
      
      builtDtypes = private$get_field_types(builtAttrs)
      builtDtypesRaw = private$get_field_types(builtAttrs, .raw = TRUE)
      
      attrStr = paste(builtAttrs, builtDtypes, collapse = ',', sep = ':')
      # convert columns to escaped strings
      colStrs = lapply(builtAttrs, function(x) {
        colScidbType = builtDtypesRaw[[x]]
        vec = df[[x]]
        switch(
          colScidbType,
          "string" = sapply(
            gsub("(['\\&])", "\\\\\\\\\\1", vec),
            function(singleStr) if(is.na(singleStr)) "" else sprintf("\\'%s\\'", singleStr)
          ),
          "datetime" = sprintf("\\'%s\\'", vec),
          "bool" = tolower(vec),
          vec # should be numeric types
        )
      })
      names(colStrs) = builtAttrs
      glueTemplate = sprintf("(%s)", paste("{", names(df), "}", sep = '', collapse = ","))
      contentStr = glue::glue_collapse(
        glue::glue_data(colStrs, glueTemplate, .sep = ',', .na = ''),
        sep = ','
      )
      if(!as_scidb_data_frame){
        builtDtypes[[artificial_field]] = 'int64'
        private$create_new(
          sprintf("build(<%s>[%s], '[%s]', true)", 
                attrStr, artificial_field, contentStr),
          dims = artificial_field,
          attrs = builtAttrs,
          dtypes = builtDtypes
        )
      } else { # build as a scidb data frame
        private$create_new(
          sprintf("build(<%s>, '[[%s]]', true)", 
                attrStr, contentStr),
          dims = c("$inst", "$seq"),
          attrs = builtAttrs,
          dtypes = builtDtypes
        )
      }
    }
    ,
    # @description 
    # Create a new ArrayOp instance by loading a file and checking it against an ArrayOp template (self).
    #
    # The ArrayOp instance where this function is called from serves as a template. By defulat, it assumes file 
    # column headers match the template's dims and attrs; otherwise an explicit file_headers can be provided and will 
    # be used to match the template's schema. 
    # @param filepath A single file path
    # @param aio_settings Customized settings of aio_input
    # @param field_conversion If NULL (default), use template's field type to convert aio_input attributes; Or provide
    # a list for customized field conversion
    # @param file_headers Column headers of the input file regardless of whether there is a header line in the file.
    # Default NULL assumes file headers match self$dims_n_attrs. If the headers order are different or there are 
    # columns to skip, please provide a string vector, in which case only columns with matching template field are 
    # loaded. Names of the unmatching column headers are irrelevant. 
    #
    # @return A new ArrayOp instance with matching fields
    load_file = function(filepath, aio_settings = NULL, field_conversion = NULL, file_headers){

      lookup = structure(0:(length(file_headers) - 1), names = file_headers)
      colIndexes = vapply(self$dims_n_attrs, function(x) lookup[x], integer(1))
      colIndexes = colIndexes[!is.na(colIndexes)]

      fieldTypes = private$get_field_types(names(colIndexes), .raw = TRUE)
      
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
      
      aioExpr = afl(aflutils$join_fields(settingItems) | aio_input)
      applyExpr = afl(aioExpr | apply(aflutils$join_fields(names(fieldTypes), castedItems)))
      projectedExpr = afl(applyExpr | project(names(fieldTypes)))
      return(private$create_new(projectedExpr, c(), names(fieldTypes), dtypes = fieldTypes))
    }
    ,
    # reshape array attributes by name and expression
    # 
    # Do not infer field data type because field can be mutated to a differnt dtype.
    # Do not alter dimensions
    # @param field_vec A named string vector
    # @return A new transient array_op
    reshape_fields = function(field_vec) {
      field_names = names(field_vec)
      assert_inherits(field_vec, 'character')
      assert_not_empty(field_names)
      assertf(!any(is.na(field_vec)) && !any(is.na(field_names)),
              glue("No NA allowed in either field names or expressions: {field_vec}"))
      
      # existing dims are also regarded as new fields
      newFileds = field_vec[field_names %-% self$attrs] 
      
      existingFields = field_vec[field_names %n% self$attrs]
      replacedFields = existingFields[existingFields != names(existingFields)]
      passedOnFields = existingFields[existingFields == names(existingFields)]
      
      newAfl = if(.not_empty(replacedFields)){
        replaceFieldsNamesAlt = sprintf("_%s", names(replacedFields))
        afl(self | 
              apply(aflutils$join_fields(replaceFieldsNamesAlt, replacedFields)) |
              project(c(replaceFieldsNamesAlt, names(passedOnFields))) |
              apply(aflutils$join_fields(
                c(names(replacedFields), names(newFileds)),
                c(replaceFieldsNamesAlt, newFileds))) |
              project(field_names)
        )
      } else if(.not_empty(newFileds)){
        afl(self |
              apply(aflutils$join_fields(names(newFileds), newFileds)) |
              project(field_names)
        )
      } else {
        afl(self | project(field_names))
      }
      private$create_new(newAfl, dims = self$dims, attrs = field_names, 
                      dim_specs = private$get_dim_specs())
      
    }
    ,
    # Reshape an arrayOp on a compound key (consisted of one or multiple fields) according to the template (self)
    # 
    # If the operands have the same dimensions as the template, then only 'project' fields
    # If the key contains all template's dimensions, then 'redimension' the operand and 'project' away unrelated fields
    # Otherwise, 'equi_join' is needed
    # 
    # @param operand The araryOp operand
    # @param keys Which fields to identify matching cells
    # @param reserved_fields Which fields of `operand` should be reserved
    # @param .redimension_setting A list of settings if scidb redimension is needed.
    # @param .join_setting A list of settings if ArrayOp$join is needed.
    # 
    # @return an arrayOp that has the same dimensions as the template (self), only reserved fields are retained.
    key_to_coordinates = function(operand, keys, reserved_fields,
                                  .redimension_setting = NULL, .join_setting = NULL){
      if(length(operand$dims) == length(self$dims) && all(operand$dims == self$dims))
        # If all dimensions match, no need to do anything about dimensions.
        return(operand$.private$afl_project(reserved_fields))
      
      operandKeyFields = .get_element_names(keys)
      templateKeyFields = as.character(keys)
      extraFields = operand$attrs %-% keys %-% reserved_fields
      
      if(all(self$dims %in% operandKeyFields)){
        if(.not_empty(extraFields))
          # operand = operand$reshape(c(keys, reserved_fields))
          operand = operand$.private$afl_project(c(keys, reserved_fields))
        return(operand$
                 change_schema(self, strict = FALSE, .setting = .redimension_setting)$
                 # reshape(reserved_fields, .force_project = FALSE))
                 .private$afl_project(reserved_fields))
      }
      
      joinOp = private$join(
        operand$select(reserved_fields %u% (self$dims %n% operand$dims_n_attrs)),
        self$select(self$dims), 
        by.x = operandKeyFields, 
        by.y = templateKeyFields, 
        settings = .join_setting)
      joinOp$change_schema(self, strict = FALSE, .setting = .redimension_setting)
    }
    ,
    # @description 
    # Create a new ArrayOp instance that with added auto incremented fields
    # 
    # @param reference ArrayOp instance to draw existing max id from 
    # @param source_field 
    # @param ref_field 
    # @param source_start 
    # @param ref_start 
    # @param new_field 
    # @return A new arrayOp 
    set_auto_increment_field = function(reference, source_field, ref_field, source_start, ref_start, new_field = NULL) {
      refDims = ref_field %-% reference$attrs
      maxRefFields = sprintf("_max_%s", ref_field)
      aggFields = sprintf("max(%s) as %s", ref_field, maxRefFields)
      defaultOffset = ref_start - source_start
      nonDefaultOffset = 1 - source_start
      
      if(.is_empty(new_field)) new_field = ref_field
      newFieldExpr = sprintf("iif(%s is null, %s%s, %s+%s%s)", maxRefFields, 
                             source_field, .to_signed_integer_str(defaultOffset), 
                             maxRefFields, source_field, .to_signed_integer_str(nonDefaultOffset))
      
      forAggregate = if(.not_empty(refDims)) afl(reference | apply(aflutils$join_fields(refDims, refDims))) else reference
      aggregated = afl(forAggregate | aggregate(aggFields))
      crossJoined = afl(
        self |
          cross_join(aggregated) | 
          apply( 
            aflutils$join_fields(new_field, newFieldExpr)
          )
      )
      result = self$spawn(crossJoined,
                          added = new_field, 
                          dtypes = new_named_list(reference$.private$get_field_types(ref_field), new_field))
      result$.private$afl_project(result$attrs)
    }
    ,
    # @description 
    # Create a new ArrayOp instance that has an anti-collision field set according to a template arrayOp
    # 
    # The source (self) operand should have N fields given the target has N+1 dimensions. The one missing field is 
    # treated as the anti-collision field.
    #
    # @param target A target arrayOp that the source draws anti-collision dimension from.
    # @param anti_collision_field a target dimension name which exsits only to resolve cell collision 
    # (ie. cells with the same dimension coordinate).
    # @return A new arrayOp 
    set_anti_collision_field = function(target, anti_collision_field = NULL, join_setting = NULL, source_anti_collision_dim_spec = NULL) {
      matchedFieldsOfTargetDimensions = self$dims_n_attrs %n% target$dims
      assert(length(matchedFieldsOfTargetDimensions) + 1 == length(target$dims), 
             "ERROR: ArrayOp$set_anti_collision_field: incorrect number of matching fields: source has %d field(s) (SHOULD BE %d) that match %d target dimension(s).",
             length(matchedFieldsOfTargetDimensions), length(target$dims) - 1, length(target$dims))
      
      if(is.null(anti_collision_field))
        anti_collision_field = target$dims %-% matchedFieldsOfTargetDimensions
      
      assert(is.character(anti_collision_field) && length(anti_collision_field) == 1, 
             "ERROR: ArrayOp$set_anti_collision_field: anti_collission_field should be a single string, but got '%s'",
             str(anti_collision_field))
      assert(anti_collision_field %in% target$dims_n_attrs, 
             "ERROR: ArrayOp$set_anti_collision_field: param 'anti_collision_field' [%s] is not a valid field of target.", anti_collision_field)
      
      # Target dimensions other than the anti_collision_field
      regularTargetDims = target$dims %-% anti_collision_field 
      
      # Redimension source with a different field name for the anti-collision-field
      srcAltId = sprintf("src_%s_", anti_collision_field) # this field is to avoid dimension collision within source 
      renamedList = as.list(structure(srcAltId, names = anti_collision_field))
      renamedTarget = target$spawn(renamed = renamedList)
      redimensionTemplateDimSpecs =
        if (is.null(source_anti_collision_dim_spec))
          renamedTarget$.private$get_dim_specs()
      else
        utils::modifyList(renamedTarget$.private$get_dim_specs(),
                          as.list(structure(source_anti_collision_dim_spec, names = srcAltId)))
      redimensionTemplate = private$create_new("TEMPLATE", 
                                            dims = renamedTarget$dims, 
                                            attrs = self$attrs %-% renamedTarget$dims,
                                            dtypes = utils::modifyList(private$get_field_types(), renamedTarget$.private$get_field_types(renamedTarget$dims)),
                                            dim_specs = redimensionTemplateDimSpecs)
      redimenedSource = redimensionTemplate$spawn(afl(
        self | redimension(redimensionTemplate$to_schema_str()) | apply(srcAltId, srcAltId)
      ))
      
      # Get the max anti-collision-field from group aggregating the target on the remainder of target dimensions
      targetAltIdMax = sprintf("max_%s_", anti_collision_field)
      groupedTarget = private$create_new('',
        attrs = c(regularTargetDims, targetAltIdMax), dims = c('instance_id', 'value_no'),
        dtypes = c(invert.list(list("int64" = c('instance_id', 'value_no', regularTargetDims, targetAltIdMax))))
      )$spawn(afl(
        target | apply(anti_collision_field, anti_collision_field) | grouped_aggregate(
          sprintf("max(%s) as %s", anti_collision_field, targetAltIdMax), regularTargetDims)
      ))
        
      # Left join on the remainder of the target dimensions
      joinSetting = if(is.null(join_setting)) list(left_outer=1,keep_dimensions=TRUE) else {
        utils::modifyList(as.list(join_setting), list(left_outer=1, keep_dimensions=TRUE))
      }
      joined = private$join(redimenedSource$select(redimenedSource$dims_n_attrs), 
                            groupedTarget$select(targetAltIdMax), 
                            by = regularTargetDims,
                            settings = joinSetting)
      
      # Finally calculate the values of anti_collision_field
      # src's attributes (that are target dimensions) are converted to dimensions according to the target
      resultTemplate = redimensionTemplate$
        spawn(renamed = invert.list(renamedList))
      resultTemplate = resultTemplate$.private$afl_unpack()
      
      
      result = resultTemplate$
        spawn(afl(
          joined | apply(anti_collision_field, sprintf(
            "iif(%s is null, %s, %s + %s + 1)", targetAltIdMax, srcAltId, srcAltId, targetAltIdMax
          )))
        # )$reshape(resultTemplate$attrs)
        )$.private$afl_project(resultTemplate$attrs)
      
      return(result)
    }
    ,
    # @description 
    # Return AFL suitable for retrieving data.frame.
    #
    # scidb::iquery has a param `only_attributes`, which, if set TRUE, will effectively drop all dims.
    # @param drop_dims Whether self's dimensions are dropped when generating AFL for data.frame conversion
    # @return An AFL string
    to_df_afl = function(drop_dims = FALSE, artificial_field = .random_attr_name()) {
      return(private$to_afl_explicit(drop_dims, self$selected, artificial_field = artificial_field))
    }
    ,
    # @description 
    # Returns AFL when self used as an operand in another parent operation.
    #
    # By default, 1. dimensions are not dropped in parent operation; 2. no intent to select fields
    # @param drop_dims Whether self dimensions will be dropped in parent operations
    # @param selected_fields which fields are selected no matter what the parent operation is.
    # If NULL, self fields will pass on by default depending on the parent operation.
    # @return An AFL string
    to_afl_explicit = function(drop_dims = FALSE, selected_fields = NULL, artificial_field = .random_attr_name()){
      
      # No intent to select fields
      if(.is_empty(selected_fields))
        return(self$to_afl())
      
      # 'selected_fields' is specified in sub-classes.
      # We need to ensure selected fields are passed on in parent operations.
      
      # dims are dropped in parent operations.
      if(drop_dims){
        selectedDims = base::intersect(selected_fields, self$dims)
        inner = if(.not_empty(selectedDims))
          afl(self | apply(aflutils$join_fields(selectedDims, selectedDims)))
        else self$to_afl()
        return(afl(inner | project(aflutils$join_fields(selected_fields))))
      }
      
      # drop_dims = F. All dims are preserved in parent operations, we can only select/drop attrs.
      selectedAttrs = base::intersect(selected_fields, self$attrs)
      if(.not_empty(selectedAttrs))  # If some attributes selected
        return(afl(self | project(selectedAttrs)))
      
      # If no attributes selected, we have to create an artificial attribute, then project it.
      # Because 'apply' doesn't work on array dimensions
      return(afl(self | apply(artificial_field, 'null') |
                   project(artificial_field)))
    }
    ### Implement raw AFL function ----
    # Functions prefixed with 'afl_' are implemented according to scidb operators with sanity checks.
    ,
    # Project a list of array's attributes.
    # 
    # Return a result array instance with the same dimensions and a subset (projected) attributes
    # If length(c(...)), then result self
    # Throw an error if there are non-attribute fileds because scidb only allows project'ing on attributes
    # Dimensions canont be projected
    # 
    # param ... c(...) is used as projected fields. NULL is discarded.
    afl_project = function(...) {
      fields = c(...)
      if(.is_empty(fields)) return(self)
      assert_inherits(fields, "character")
      assert_empty(fields %-% self$attrs, "afl_project: invalid field(s) to project: [{.value}]")
      # assert_not_has_len(nonAttrs, "ERROR: afl_project: %d non-attribute field(s) found: %s", length(nonAttrs), paste(nonAttrs, collapse = ', '))
      private$create_new(afl(self | project(fields)), dims = self$dims, attrs = fields, 
                      dtypes = private$get_field_types(c(self$dims, fields)), dim_specs = private$get_dim_specs())
    }
    ,
    # AFL unpack operator
    # 
    # Return an array_op with self's dims + attrs, plus an extra dimension
    afl_unpack = function(dim_name = dbutils$random_field_name(), chunk_size = NULL) {
      private$create_new(afl(self | unpack(c(dim_name, chunk_size))), 
                      dims = dim_name,
                      attrs = self$dims_n_attrs,
                      dtypes = private$get_field_types())
    }
    # ,
    # # Apply new attributes to an existing array. 
    # # Return a result array with added (applied) attributes
    # # If fields are existing dimensions, data types are inheritted; otherwise new attributes require data types
    # # @param fields: a named list or character. Cannot contain existing attributes because it creates conflicts.
    # afl_apply = function(fields, dtypes = NULL) {
    #   fieldNames = .get_element_names(fields)
    #   fieldExprs = as.character(fields)
    #   conflictFields = fields %n% self$attrs
    #   assert_not_has_len(conflictFields, "ERROR: afl_apply: cannot apply existing attribute(s): %s", paste(conflictFields, collapse = ', '))
    #   
    #   newDTypes = utils::modifyList(private$get_field_types(), as.list(dtypes))
    #   private$create_new(afl(self | apply(aflutils$join_fields(fieldNames, fieldExprs))), dims = self$dims, 
    #                   attrs = self$attrs %u% fields, dtypes = newDTypes, dim_specs = private$get_dim_specs())
    # }
    ,
    # Redimension `self` according to a template.
    # 
    # @param .setting a string vector, where each item will be appended to the redimension operand.
    # E.g. .setting = c('false', 'cells_per_chunk: 1234') ==> redimension(source, template, false, cells_per_chunk: 1234)
    # Similar to scidb redimension operator
    afl_redimension = function(template, .setting = NULL) {
      assert_no_fields(template$dims_n_attrs %-% self$dims_n_attrs, 
                       "ERROR:ArrayOp$afl_redimension:Field(s) '%s' of the template not found in the source.")
      return(template$spawn(afl(
        self | redimension(c(template$to_schema_str(), as.character(.setting)))
      )))
    }
    ,
    # Insert to a target array (scidb `insert` operator)
    # 
    # Require numbers of attributes and dimensions of self and target match.
    # Field names are irrelevant
    # 
    # @param target: Target array
    # @return An scidb insert operation
    afl_insert = function(target) {
      assertf(length(self$dims) == length(target$dims), 
              glue("Number of dimensions mismatch between ",
              "source array {length(self$dims)} [{paste(self$dims, collapse=',')}] and ",
              "target array {length(target$dims)} [{paste(target$dims, collapse=',')}]"),
              .nframe = 1)
      assertf(length(self$attrs) == length(target$attrs), 
              glue("Number of attributes mismatch between ",
              "source array {length(self$attrs)} [{paste(self$attrs, collapse=',')}] and ",
              "target array {length(target$attrs)} [{paste(target$attrs, collapse=',')}]"),
              .nframe = 1)
      target$spawn(afl(self | insert(target)))
    }
    ,
    # Overwrite a target array (scidb `store` operator)
    # 
    # Require numbers of attributes and dimensions of self and target match.
    # Field names are irrelevant
    # 
    # @param target: Target array
    # @return An scidb insert operation
    afl_store = function(target, new_target = FALSE) {
      assertf(length(self$dims) == length(target$dims), 
              glue("Number of dimensions mismatch between ",
              "source array {length(self$dims)} [{paste(self$dims, collapse=',')}] and ",
              "target array {length(target$dims)} [{paste(target$dims, collapse=',')}]"),
              .nframe = 1)
      assertf(length(self$attrs) == length(target$attrs), 
              glue("Number of attributes mismatch between ",
              "source array {length(self$attrs)} [{paste(self$attrs, collapse=',')}] and ",
              "target array {length(target$attrs)} [{paste(target$attrs, collapse=',')}]"),
              .nframe = 1)
      target$spawn(afl(self | store(target)))
    }
  )
  ,
  # Active bindings ----
  active = list(
    #' @field dims Dimension names
    dims = function() private$get_meta('dims'),
    #' @field attrs Attribute names
    attrs = function() private$get_meta('attrs'),
    #' @field selected Selected dimension and/or attribute names
    selected = function() private$get_meta('selected'),
    #' @field dtypes A named list, where key is dim/attr name and value is respective SciDB data type as string
    dtypes = function() private$get_meta('dtypes'),
    #' @field raw_dtypes A named list, where key is dim/attr name and value is first part of respective SciDB data type as string
    raw_dtypes = function() lapply(private$get_meta('dtypes'), function(x) gsub("^(\\S+).*$", "\\1", x)),
    #' @field dims_n_attrs Dimension and attribute names
    dims_n_attrs = function() c(self$dims, self$attrs),
    #' @field attrs_n_dims Attribute and dimension names
    attrs_n_dims = function() c(self$attrs, self$dims),
    #' @field is_schema_from_scidb If the array schema is retrieved from SciDB or inferred locally in R
    is_schema_from_scidb = function() private$get_meta('is_schema_from_scidb') %?% FALSE,
    #' @field is_scidb_data_frame Whether current array_op is a regular array or
    #' SciDB data frame (array with hidden dimensions; not to be confused with R data frames)
    is_scidb_data_frame = function() {
      # True if any dimension starts with $
      any(grepl("^\\$", self$sync_schema()$dims))
    },
    #' @field .private For internal testing only. Do not access this field to avoid unintended consequences!!!
    .private = function() private
  ),


  # Public ----------------------------------------------------------------------------------------------------------

  public = list(
    #' @description 
    #' Base class initialize function, to be called in sub-class internally.
    #' 
    #' Always use `ScidbConnection` to get array_op instances.
    #' @param raw_afl AFL expression (array name or operations) as string
    #' @param dims A string vector used as dimension names
    #' @param attrs A string vector used as attribute names
    #' @param dtypes A named list of strings, where names are attribute names and
    #' values are full scidb data types. 
    #' E.g. `dtypes = list(field_str = "string NOT NULL", field_int32 = "int32")`
    #' @param dim_specs A named list of string, where names are dimension names 
    #' and values are dimension specs.
    #' E.g. `dim_sepcs = list(da = "0:*:0:*", chrom = "1:24:0:1")`. 
    #' @param ... A  named list of metadata items, where names are used as keys
    #' in `private$set_meta` and `private$get_meta` functions.
    #' @param meta_list A list that stores ArrayOp meta data, e.g. field types 
    #' If provided, other regular parms are not allowed.
    initialize = function(
      raw_afl,
      dims = as.character(c()), 
      attrs = as.character(c()),
      dtypes = list(),
      dim_specs = list(),
      ...,
      meta_list
    ) {
      assert(
        xor(methods::hasArg('meta_list'), 
              methods::hasArg('dims') || methods::hasArg('attrs') || methods::hasArg('dtypes')
        ),
      "ERROR: ArrayOp:initialze: meta_list cannot be provided with any of the args: dims, attrs, dtypes")
      assert_single_str(raw_afl, "character")
      
      private$raw_afl = raw_afl
      private$metaList = if(methods::hasArg('meta_list')) meta_list else
        list(dims = dims, attrs = attrs, 
             dtypes = utils::modifyList(dtypes, invert.list(list("int64" = dims))), 
             dim_specs = dim_specs, ...)
    }
    # Functions that create new ArrayOps -------------------------------------------------------------------------------
    ,
    #' @description 
    #' Create a new ArrayOp instance with filter expressions
    #' 
    #' Similar to `dplyr::filter`, fields are not quoted. 
    #' 
    #' Operators for any type of fields include `==`, `!=`, 
    #' `%in%`, `%not_in%`.
    #' To test whether a field is null, use unary operators: `is_null`, `not_null`.
    #'
    #' Special binary operators for string fields include:
    #' `%contains%`, `%starts_with%`, `%ends_with%`, `%like%`, where 
    #' only `%like%` takes a regular expression and other operators escape any special
    #' characters in the right operand.
    #' 
    #' Operators for numeric fields include: `>`, `<`, `>=`, `<=`
    #' 
    #' @param ... Filter expression(s) in R syntax. 
    #' These expression(s) are not evaluated in R but first captured then converted to scidb expressions with appropriate syntax.
    #' @param .expr A single R expression, or a list of R exprs, or NULL. 
    #' If provided, `...` is ignored. Mutiple exprs are joined by 'and'.
    #' This param is useful when we want to pass an already captured R expression.
    #' @param .validate_fields Boolean, default TURE, whether to validate fields 
    #' in filter epxressions. Throw error if invalid fields exist when set to TRUE. 
    #' @param .regex_func A string of regex function implementation, default 'regex'.
    #' Due to scidb compatiblity issue with its dependencies, the regex function from boost library may not be available
    #' Currently supported options include 'rsub', and 'regex'
    #' @param .ignore_case A Boolean, default TRUE. If TRUE, ignore case in string match patterns. 
    #' Otherwise, perform case-sensitive regex matches.
    #' @return A new arrayOp 
    filter = function(..., .expr = NULL, .validate_fields = TRUE, 
                     .regex_func = getOption('arrayop.regex_func', default = 'regex'), 
                     .ignore_case = getOption('arrayop.ignore_case', default = TRUE)) {
      filterExpr = .expr %?% aflutils$e_merge(aflutils$e(...))
      # No actual filter expression
      if(.is_empty(filterExpr)) return(self)
      
      # If we need to validate every field is actually one of self's fields
      if(.validate_fields){
        status = aflutils$validate_filter_expr(filterExpr, self$dims_n_attrs)
        if(!status$success){
          # either invalid fields or R expression syntax error
          assert_empty(status$absent_fields,
                       "invalid field(s) [{.value}] in filter expression")
          stop(paste(status$error_msgs, collapse = '\n'))
        }
      }
      
      self$spawn(afl(self | 
                       filter(aflutils$e_to_afl(filterExpr, 
                                                regex_func = .regex_func, 
                                                ignore_case = .ignore_case))))
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with mutated fields
    #' 
    #' Similar to `dplyr::mutate`, fields of source (self) can be removed or added to the result arrayOp
    #' Any field that are not in the mutate expressions remain unchanged.
    #' 
    #' @param ... Named R expressions. Names are field names in the result arrayOp and must not be empty.
    #' Set field = NULL to remove existing fields. E.g. `abcd = NULL, def = def` removes
    #' field 'abcd' and keep field 'def'. 
    #' 
    #' Values are R expressions similar to the `filter` method. 
    #' E.g. `a = b + 2, name = first + "-" + last, chrom = if(chrom == 'x') 23 else if(chrom == 'y') 24 else chrom`
    #' @param .dots A named list of R expressions or NULL. If provided,
    #' the `...` param is ignored. Useful when an a list of mutation expressions is already 
    #' created and can be passed around.
    #' @param .sync_schema Whether to get the exact schema from scidb. Default 
    #' TRUE will cause a scidb query to get the schema. Set to FALSE to avoid schema checking.
    #' 
    #' @return a new ArrayOp instance
    mutate = function(..., .dots = NULL, .sync_schema = TRUE){
      
      paramFieldExprs = lapply(.dots %?% rlang::exprs(...), aflutils$e_to_afl)
      assert_unique_named_list(
        paramFieldExprs,
        "'mutate fields' should be a named list where each element has a unique name. E.g. mutate(field_name = 'field_expression') "
      )
      assertf(all(sapply(paramFieldExprs, function(x) is.null(x) || is.character(x))),
              "Each element should be a string or NULL")
      assert_empty(
        names(Filter(is.null, paramFieldExprs)) %-% self$attrs,
        "mutate: Can only remove array attributes, but got invalid field(s) for removal: [{.value}]"
      )
      
      fieldList = utils::modifyList(new_named_list(self$attrs), paramFieldExprs)
      assert_not_empty(fieldList, 
                       paste0("mutate: no valid fields found.", 
                              "Please set at least one field, e.g. array_op$mutate(existing_field = NULL, placeholder=bool(null))"))
      fields_vec = structure(as.character(fieldList), names = names(fieldList))
      
      result = private$reshape_fields(fields_vec)
      if(.sync_schema) result$sync_schema() else result
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with mutated fields
    #' 
    #' Similar to `dplyr::transmute`, only listed fields are retained in the result arrayOp
    #' NOTE: Any field that are not in the mutate expressions will be discarded.
    #' 
    #' @param ... R expressions. Names are optional. 
    #' For each named expression, the name is used as field name in the result arrayOp. 
    #' Unnamed expressions must be existing field names, unquoted, which result in unchanged source fields of self.
    #' 
    #' Values are R expressions similar to the `filter` method. 
    #' E.g. `a = b + 2, name = first + "-" + last, chrom = if(chrom == 'x') 23 else if(chrom == 'y') 24 else chrom`
    #' @param .dots A named list of R expressions or NULL. If provided,
    #' the `...` param is ignored. Useful when an a list of mutation expressions is already 
    #' created and can be passed around.
    #' @param .sync_schema Whether to get the exact schema from scidb. Default 
    #' TRUE will cause a scidb query to get the schema. Set to FALSE to avoid schema checking.
    #' 
    #' @return a new ArrayOp instance
    transmute = function(..., .dots = NULL, .sync_schema = TRUE){
      paramFieldExprs = lapply(.dots %?% rlang::exprs(...), aflutils$e_to_afl)
      assertf(all(sapply(paramFieldExprs, is.character)),
              glue(
              "'transmute fields' should be a list of non-nullable strings with optional names. E.g. transmute('a', b='a + 2').",
              " But got: {deparse(paramFieldExprs)}"
              ))
      
      paramFieldNames = names(paramFieldExprs) %?% paramFieldExprs
      names(paramFieldExprs) = ifelse(paramFieldNames == "", paramFieldExprs, paramFieldNames)
      
      assert_unique_named_list(
        paramFieldExprs,
        glue(
          "'transmute fields' should be a named list where each element has a unique name. E.g. mutate(field_name = 'field_expression') ",
          " But got: {deparse(paramFieldExprs)}"
        )
      )
      fields_vec = structure(as.character(paramFieldExprs), names = names(paramFieldExprs))
      result = private$reshape_fields(fields_vec)
      if(.sync_schema) result$sync_schema() else result
    }
    ,
    #' @description 
    #' Create a ArrayOp instance with the same schema of self, but different cells
    #' from 'data_array' for the 'updated_fields'.
    #' 
    #' @param data_array An ArrayOp instance that have at least two overlapping fields with self.
    #' @param keys Field names in both self and data_array. Cell content of these
    #' fields are from the 'self' arrayOp rather than 'data_array'.
    #' @param updated_fields Field names in both self and data_array. Cell content
    #' of these fields are from the 'data_array', NOT 'self'.
    #' @param .redimension_setting A list of strings used as the settings of scidb
    #' 'redimension' operator. Only applicable when a 'redimension' is needed.
    #' @param .join_setting A list of strings used as the settings of scidb
    #' 'join' operator. Only applicable when a 'join' is needed.
    #' @return A new arrayOp with the same schema as self
    mutate_by = function(data_array, 
                         keys = NULL, 
                         updated_fields = NULL,
                         .redimension_setting = NULL, 
                         .join_setting = NULL) {
      assert_inherits(data_array, "ArrayOpBase")
      
      data_source = data_array
      
      # updateFields default to the overlapping attrs between source and self
      # updatedFields = if(.not_empty(updated_fields)) updated_fields else data_source$attrs %n% self$attrs
      updatedFields = updated_fields %?% data_source$attrs %n% self$attrs
      assert_not_empty(updatedFields, "param 'data_source' does not have any target attributes to mutate.")
      reservedFields = self$attrs %-% updatedFields
      sameDims = length(data_source$dims) == length(self$dims) && all(data_source$dims == self$dims)
      
      if(!sameDims){ # not the same dimensions, transform the data_source first
        assert_not_empty(keys, "param 'keys' cannot be empty")
        assert_not_empty(updated_fields, "param 'updated_fields' cannot be empty")
        assert_no_fields(
          (keys %-% self$dims_n_attrs) %u% (keys %-% data_source$dims_n_attrs),
          "param 'keys' has invalid field(s) [%s]")
        assert_no_fields(
          (updated_fields %-% self$dims_n_attrs) %u% (updated_fields %-% data_source$dims_n_attrs),
          "param 'updated_fields' has invalid field(s) [%s]")
        data_source = private$key_to_coordinates(data_source, keys = keys, reserved_fields = updatedFields, 
                                                 .redimension_setting = .redimension_setting, .join_setting = .join_setting)
      } else {
        if(.not_empty(keys %-% self$dims)) 
          warning(sprintf("Extra key(s) [%s] ignored because source/target dimensions match.",
                          keys %-% self$dims))
      }
      if(.is_empty(reservedFields)){
        # No need to join with self. Just ensure ordering of data_source attributes conforms to 'self'
        data_source$.private$afl_project(self$attrs)
      } else {
        self$spawn(
          afl(
            data_source$.private$afl_project(updatedFields) | 
              join(private$afl_project(reservedFields))
          )
        )$.private$afl_project(self$attrs)
      }
    }
    ,
    #' @description 
    #' Inner join two arrays: 'self' (left) and 'right' 
    #' 
    #' Similar to `dplyr::inner_join`, the result arrayOp performs an inner join.
    #' For both left and right arrays, only selected fields are included in the result arrayOp.
    #' If no fields are selected, then all fields are treated as selected. 
    #' 
    #' @param right An arrayOp instance
    #' @param by.x NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by.y NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'. If not NULL,
    #' must be fields of both operands.
    #' @param left_alias Alias for left array to resolve potential conflicting fields in result
    #' @param right_alias Alias for right array to resolve potential conflicting fields in result
    #' @param join_mode String 'equi_join' or 'cross_join'. The latter requires
    #' join keys are all dimensions of both operands, which is more strigenet than 'equi_join' 
    #' with the benifit of non-materializing result in scidb.
    #' @param settings A named list as join settings. E.g. ` list(algorithm = "'hash_replicate_right'")`
    #' @return A new arrayOp instance 
    inner_join = function(right, 
                          by.x = NULL, by.y = NULL, by = NULL, 
                          left_alias = '_L', right_alias = '_R',
                          join_mode = 'equi_join',
                          settings = NULL
                          ) {
      
      private$join(self, right,
                   by.x = by.x, by.y = by.y, by = by,
                   left_alias = left_alias, right_alias = right_alias,
                   join_mode = join_mode,
                   settings = settings
                   )  
    }
    ,
    #' @description 
    #' Left join two arrays: 'self' (left) and 'right' 
    #' 
    #' Similar to `dplyr::left_join`, the result arrayOp performs a left join.
    #' For both left and right arrays, only selected fields are included in the result arrayOp.
    #' If no fields are selected, then all fields are treated as selected. 
    #' 
    #' @param right An arrayOp instance
    #' @param by.x NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by.y NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'. If not NULL,
    #' must be fields of both operands.
    #' @param left_alias Alias for left array to resolve potential conflicting fields in result
    #' @param right_alias Alias for right array to resolve potential conflicting fields in result
    #' @param settings A named list as join settings. E.g. ` list(algorithm = "'hash_replicate_right'")`
    #' @return A new arrayOp instance 
    left_join = function(right, 
                         by.x = NULL, by.y = NULL, by = NULL, 
                         left_alias = '_L', right_alias = '_R',
                         settings = NULL
                         ) {
      joinSettings = utils::modifyList(as.list(settings), list(left_outer = 1))
      private$join(self, right,
                   by.x = by.x, by.y = by.y, by = by,
                   left_alias = left_alias, right_alias = right_alias,
                   settings = joinSettings,
                   join_mode = 'equi_join'
                   )  
    }
    ,
    #' @description 
    #' Right join two arrays: 'self' (left) and 'right' 
    #' 
    #' Similar to `dplyr::right_join`, the result arrayOp performs a right join.
    #' For both left and right arrays, only selected fields are included in the result arrayOp.
    #' If no fields are selected, then all fields are treated as selected. 
    #' 
    #' @param right An arrayOp instance
    #' @param by.x NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by.y NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'. If not NULL,
    #' must be fields of both operands.
    #' @param left_alias Alias for left array to resolve potential conflicting fields in result
    #' @param right_alias Alias for right array to resolve potential conflicting fields in result
    #' @param settings A named list as join settings. E.g. ` list(algorithm = "'hash_replicate_right'")`
    #' @return A new arrayOp instance 
    right_join = function(right, 
                          by.x = NULL, by.y = NULL, by = NULL, 
                          left_alias = '_L', right_alias = '_R',
                          settings = NULL
                          ) {
      joinSettings = utils::modifyList(as.list(settings), list(right_outer=1))
      private$join(self, right,
                   by.x = by.x, by.y = by.y, by = by,
                   left_alias = left_alias, right_alias = right_alias,
                   settings = joinSettings,
                   join_mode = 'equi_join'
                   )  
    }
    ,
    #' @description 
    #' Full join two arrays: 'self' (left) and 'right' 
    #' 
    #' Similar to `dplyr::full_join`, the result arrayOp performs a full join.
    #' For both left and right arrays, only selected fields are included in the result arrayOp.
    #' If no fields are selected, then all fields are treated as selected. 
    #' 
    #' @param right An arrayOp instance
    #' @param by.x NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by.y NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'.
    #' @param by NULL or a string vector as join keys. If set to NULL, join
    #' keys are inferred as shared fields of 'left' and 'right'. If not NULL,
    #' must be fields of both operands.
    #' @param left_alias Alias for left array to resolve potential conflicting fields in result
    #' @param right_alias Alias for right array to resolve potential conflicting fields in result
    #' @param settings A named list as join settings. E.g. `list(algorithm = "'hash_replicate_right'")`
    #' @return A new arrayOp instance 
    full_join = function(right, 
                          by.x = NULL, by.y = NULL, by = NULL, 
                          left_alias = '_L', right_alias = '_R',
                          settings = NULL
                          ) {
      joinSettings = utils::modifyList(as.list(settings), list(left_outer = 1, right_outer=1))
      private$join(self, right,
                   by.x = by.x, by.y = by.y, by = by,
                   left_alias = left_alias, right_alias = right_alias,
                   settings = joinSettings,
                   join_mode = 'equi_join'
                   )  
    }
    ,
    #' @description 
    #' Return an arrayOp instance with same schema as self and content cells that
    #' match the cells of 'df_or_arrayop'.
    #' 
    #' Similar to `dplyr::semi_join`, the result has the same schema as the left
    #' operand 'self' and with content filtered by 'df_or_arrayop'.
    #' 
    #' params `field_mapping`, `lower_bound` and `upper_bound`, if provided, must be named list,
    #' where the.names are from the source array (i.e. self), and values are from 
    #' the right operand `df_or_arrayop`
    #' @param field_mapping NULL or a named list of strings. Only applicable when
    #' mode is 'cross_between' mode, ignored in other modes.
    #' @param lower_bound NULL or a named list of strings. Only applicable when
    #' mode is 'filter' or 'cross_between'. Names of the list are fields of self,
    #' and value strings are fields or columns of the `df_or_arrayop` which are
    #' treated as lower bound to matching fields rather than exact match.
    #' 
    #' In 'filter' mode, the self fields in lower_bound can be any numeric fields. 
    #' In 'cross_between' mode, the self fields in lower_bound must be array dimensions.
    #' @param upper_bound NULL or a named list of strings. Only applicable when
    #' mode is 'filter' or 'cross_between'. Names of the list are fields of self,
    #' and value strings are fields or columns of the `df_or_arrayop` which are
    #' treated as upper bound to matching fields rather than exact match.
    #' 
    #' In 'filter' mode, the self fields in upper_bound can be any numeric fields. 
    #' In 'cross_between' mode, the self fields in upper_bound must be array dimensions.
    #' @param df_or_arrayop An R data frame or arrayOp instance.
    #' @param mode String of 'filter', 'cross_between', 'index_lookup' or 'auto'
    #' @param filter_threshold A number below which the 'filter' mode is used
    #' unless a mode other than 'auto' is provided. 
    #' @param upload_threshold A number below which the 'df_or_arrayop' data frame
    #' is compield into a build literal array; otherwise uploaded to scidb as a regular array.
    #' Only applicable when 'df_or_arrayop' is an R data frame.
    #' @return An arrayOp instance
    semi_join = function(df_or_arrayop, 
                         field_mapping = NULL,
                         lower_bound = NULL,
                         upper_bound = NULL,
                         mode = "auto",
                         filter_threshold = 200L,
                         upload_threshold = 6000L
                         ){
      assert_inherits(df_or_arrayop, c("data.frame", "ArrayOpBase"))  
      assert_empty(
        (names(field_mapping) %u% names(lower_bound) %u% names(upper_bound)) %-% self$dims_n_attrs,
        "Field(s) not found in the reference array: [{.value}]"
      )
      VALID_MODES = c("auto", "filter", "cross_between", "index_lookup")
      assertf(
        mode %in% VALID_MODES,
        sprintf("Invalid param 'mode': [%s]. Should be one of [%s]", 
                mode, paste(VALID_MODES, collapse = ","))
      )
      
      op_mode = 
      if(is.data.frame(df_or_arrayop)){
        df = df_or_arrayop
        assert_empty(
          names(df) %-% self$dims_n_attrs %-% as.character(field_mapping) %-% as.character(lower_bound) %-% as.character(upper_bound),
          "Param 'df' has invalid column(s): [{.value}]"
        )
        numCells = base::nrow(df) * length(names(df))
        numCols = length(names(df))
        if(mode == "auto") {
          if (numCells <= filter_threshold)
            "filter"
          else if (numCols == 1)
            "index_lookup"
          else
            "cross_between"
        } else {
          mode
        }
      } else { # rhs is array_op
        if(mode == "auto"){
          if(length(df_or_arrayop$dims_n_attrs) == 2L)
            "index_lookup"
          else 
            "cross_between"
        } else {
          assertf(mode != "filter", "semi_join 'filter' mode only works with data.frame, but array_op is provided.")
          mode
        }
      }
      
      if(is.data.frame(df_or_arrayop)){
        df = df_or_arrayop
        if(op_mode == "filter"){
          return(
            private$semi_join_by_df_filter(
              df,
              field_mapping = field_mapping,
              lower_bound = lower_bound,
              upper_bound = upper_bound
            )
          )
        }
        explicitFields = as.list(c(field_mapping, lower_bound, upper_bound))
        # if(.not_empty(as.character(explicitFields) %-% names(explicitFields))) browser()
        implicitFields = names(df) %-% as.character(explicitFields)
        implicitFields = new_named_list(implicitFields, implicitFields)
        
        dfFields = as.character(explicitFields) %u% names(implicitFields)
        refFields = c(names(explicitFields), as.character(implicitFields)) # allow duplicates here
        templateDtypes = new_named_list(
          private$get_field_types(refFields, .raw = T), 
          names = dfFields
        )
        
        arrayTemplate = private$create_new(
          "", dims = "x",
          attrs = dfFields,
          dtypes = templateDtypes
        )
        # build_or_upload_df(df, arrayTemplate, threshold = upload_threshold)
        dataArray = private$conn$array_from_df(df, arrayTemplate, build_or_upload_threshold = upload_threshold)
      } else {
        dataArray = df_or_arrayop
      }
      
      result = private$match(dataArray, op_mode = op_mode, 
                             field_mapping = field_mapping,
                             lower_bound = lower_bound,
                             upper_bound = upper_bound)
      # Add to ref count to avoid R's GC
      result$.private$hold_refs(dataArray)
      return(result)
    }
    ,
    #' @description 
    #' Create a new arrayOp with 'group by' fields
    #' 
    #' The result arrayOp is identical to self except for the 'group_by' fields.
    #' When called before `summarize` function, result arrayOp will be converted into `grouped_aggregate` operation.
    #' @param ... field names as strings or string vectors, which will be merged 
    #' into a single string vector with `c(...)`
    #' @return An arrayOp instance with group_by fields
    group_by = function(...) {
      fields = c(...)
      assert_inherits(fields, "character", .symbol = "group_by_fields")
      assert_empty(fields %-% self$dims_n_attrs, 
                   "invalid group_by field(s): [{.value}]")
      result = self$spawn(self$to_afl())
      result$.private$set_meta('group_by_fields', unique(fields))
      result
    }
    ,
    #' @description 
    #' Create a new arrayOp with aggregated fields
    #' 
    #' @param ... aggregation expressions in R syntax. Names of expressions are optional.
    #' If provided, names will be the fields of result arrayOp; otherwise field
    #' names are auto generated by scidb.
    #' Same syntax as `...` in 'filter' and 'mutate' functions.
    #' @param .dots a list of aggregation expressions. Similar to '.dots' in 
    #' 'mutate' and 'transmute'.
    #' @return A new arrayOp instance
    summarize = function(..., .dots = NULL){
      group_by_fields = private$get_meta("group_by_fields")
      
      # expression list converted to a string list
      rawFieldsList = lapply(.dots %?% rlang::exprs(...), aflutils$e_to_afl)
      
      # get aggregation expressions
      aliases = names(rawFieldsList)
      agg_exprs = as.character(rawFieldsList)
      
      agg_exprs_pairs = if(!is.null(aliases)) {
        ifelse(aliases == "", agg_exprs,
               paste0(agg_exprs, ' as ', aliases))
      } else agg_exprs
      
      result = 
        if(.not_empty(group_by_fields)){
          private$conn$afl_expr(afl(
          self | grouped_aggregate(
            paste(agg_exprs_pairs, collapse = ','),
            group_by_fields
          )))
        } else {
          private$conn$afl_expr(afl(
            self | 
              apply(aflutils$join_fields(self$dims, self$dims)) |
              aggregate(paste(agg_exprs_pairs, collapse = ','))
          ))
        }
      # result$.private$set_conn(private$conn)
      result
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance that has auto incremented fields and/or anti-collision fields according to a template arrayOp
    #' 
    #' If the dimension count, attribute count and data types match between the source(self) and target, 
    #' then no redimension will be performed, otherwise redimension on the source first.
    #'
    #' Redimension mode requires all target fields exist on the source disregard of being attributes or dimensions.
    #' Redimension mode does not check on whether source data types match the target because auto data conversion 
    #' occurs within scidb where necessary/applicable. 
    #' @param target A target ArrayOp the source data is written to. 
    #' @param source_auto_increment A single named integer, a single string, or NULL.
    #' Eg. c(z=0) for field 'z' in the source (ie. self) starting from 0; 
    #' or a single string 'z' equivalent to c(z=0). If NULL, assume it to be the 
    #' only dimension in self, normally from an artificial dimension of a build literal or unpack operation.
    #' @param target_auto_increment a named number vector or string vector or NULL.
    #' where the name is a target field and value is the starting index.
    #' E.g. c(aid=0, bid=1) means to set auto fields 'aid', 'bid' according to the target fields of the same name.
    #' If 'target' doesn't have a cell, then default values start from 0 and 1 for aid and bid, respectively.
    #' A string vector c("aid", "bid") is equvilant to c(aid=0, bid=0).
    #' NULL means treat all missing fields (absent in self but present in target) as 0-based auto increment fields.
    #' Here the `target_auto_increment` param only affects the initial load when the field is still null in the target array.
    #' @param anti_collision_field a target dimension name which exsits only to resolve cell collision 
    #' (ie. cells with the same dimension coordinate).
    #' @param join_setting NULL or a named list. When not NULL, it is converted 
    #' to settings for scidb `equi_join` operator, only applicable when 
    #' `anti_collision_field` is not NULL.
    #' @param source_anti_collision_dim_spec NULL or a string. 
    #' If NULL, the dimension spec for the anti-collision dimension in source 
    #' (self) is taken from self's schema. 
    #' In rare cases, we need to set the dimension spec to control the chunk size
    #' in the 'redimension' operation, e.g. `source_anti_collision_dim_spec = "0:*:0:123456"`
    #' @return A new arrayop instance
    set_auto_fields = function(target,
                               source_auto_increment = NULL,
                               target_auto_increment = NULL,
                               anti_collision_field = NULL,
                               join_setting = NULL,
                               source_anti_collision_dim_spec = NULL)
    {
      assert_inherits(target, "ArrayOpBase")
      
      result = self
      
      assertf(length(source_auto_increment) == 1L || length(source_auto_increment) == 0L, 
              "source_auto_increment should be a single value of string or integer")
      if(.is_empty(source_auto_increment)){
        # if the param is not provided, we assume the source array has only 
        # one 0-based dimension which is used as the source_auto_increment
        assertf(length(self$dims) == 1L, 
                paste0("Cannot infer param source_auto_increment.", 
                "Please provide `c(z=1)` if `z` is a field in the source array starting from 1;",
                "or `'z'` if z starts from 0."))
        source_auto_increment = structure(0L, names = self$dims)
      } else {
        # if auto increment fields are strings, assume they are 0-based
        if(is.character(source_auto_increment) && is.null(names(source_auto_increment))) 
          source_auto_increment = structure(0, names = source_auto_increment)
        else{
          source_auto_increment = structure(as.integer(source_auto_increment), names = names(source_auto_increment))
        }
      }
      assertf(!any(is.na(source_auto_increment)), 
              "Invalid param 'source_auto_increment': must be a named integer where name is the field name, and value is the field starting value")
      srcIncName = names(source_auto_increment)
      assertf(.not_empty(srcIncName) && srcIncName %in% self$dims_n_attrs,
              glue("source_auto_increment is not a valid field: {srcIncName}"))
      
      # Infer target_auto_increment fields
      if(.is_empty(anti_collision_field) &&
          .is_empty(target_auto_increment)){
          missingTargetFields = target$dims_n_attrs %-% self$dims_n_attrs
          assert_not_empty(missingTargetFields,
                           "Cannot not infer param target_auto_increment because there are no fields in target array for the source array to add.")
          target_auto_increment = structure(rep(0L, length(missingTargetFields)),
                                            names = missingTargetFields)
      }
      # Infer target_auto_increment fields which can be mutiple, unlike source_auto_increment.
      if(is.character(target_auto_increment) &&
         is.null(names(target_auto_increment))) {
        target_auto_increment = structure(rep(0L, length(target_auto_increment)),
                                          names = target_auto_increment)
      }
      
      # If there is an auto-incremnt field, it needs to be inferred from the params
      # After this step, 'src' is an updated ArrayOp with auto-incremented id calculated (during AFL execution only due to lazy evaluation)
      if(.not_empty(target_auto_increment)){
        result = result$.private$set_auto_increment_field(target, 
          source_field = names(source_auto_increment), ref_field = names(target_auto_increment), 
          source_start = source_auto_increment, ref_start = target_auto_increment)
      }
      
      # If there is anti_collision_field, more actions are required to avoid cell collision
      # After this step, 'src' is an updated ArrayOp with auto_collision_field set properly
      if(.not_empty(anti_collision_field)){
        result = result$.private$set_anti_collision_field(target, anti_collision_field, join_setting = join_setting, 
                                                 source_anti_collision_dim_spec = source_anti_collision_dim_spec)
      }
      result = private$conn$afl_expr(result$to_afl())
      return(result)
    }
    ,
    #' @description 
    #' Update the target array with self's content
    #' 
    #' Similar behavior to scidb insert operator.
    #' Require numbers of attributes and dimensions of self and target arrays match.
    #' Field names are irrelevant.
    #' 
    #' This function only returns an arrayOp with the update operation AFL 
    #' encapsulated. No real action is performed in scidb until 
    #' `source$update(target)$execute()` is called.
    #' 
    #' @param target An arrayOp instance where self's content is updated. Must be
    #' a persistent array, since it is meanlingless to update an array operation.
    #' @return A new arrayOp that encapsulates the update operation
    update = function(target) {
      assert_inherits(target, "ArrayOpBase")
      assertf(target$is_persistent(), "update: param 'target' must be a persistent array")
      private$afl_insert(target)
    }
    ,
    #' @description 
    #' Overwrite the target array with self's content
    #' 
    #' Similar behavior to scidb store operator.
    #' Require numbers of attributes and dimensions of self and target arrays match.
    #' Field names are irrelevant.
    #' 
    #' This function only returns an arrayOp with the update operation AFL 
    #' encapsulated. No real action is performed in scidb until 
    #' `source$overwrite(target)$execute()` is called.
    #' 
    #' Warning: Target's content will be erased and filled with self's content.
    #' 
    #' @param target An arrayOp instance where self's content is written to.
    #' Must be a persitent array either preexist or does not exist in scidb.
    #' @return A new arrayOp that encapsulates the overwrite operation
    overwrite = function(target) {
     assert_inherits(target, "ArrayOpBase")
      assertf(target$is_persistent(), "update: param 'target' must be a persistent array")
      private$afl_store(target)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with selected fields
    #' 
    #' NOTE: this does NOT change the to_afl output, but explicitly state which field(s) should be retained if used in
    #' a parent operation that changes its schema, e.g. `inner_join`, `left_join`, `right_join` and `to_df`.
    #' 
    #' The `select`ed fields are passed on to derived ArrayOp instances. 
    #' 
    #' In all join operations, if no field is explicitly `select`ed, then all fields are assumed be retained.
    #' In `to_df`, if no field is explicitly `select`ed, only the attributes are retrieved as data frame columns.
    #' In `to_df_all`, if no field is explicitly `select`ed, it is equivalent to select all dimensions and attributes.
    #' @param ... Which field(s) to retain.
    #' @return A new arrayOp 
    select = function(...) {
      fieldNames = c(...)
      if(!is.null(fieldNames)) assert_inherits(fieldNames, "character")
      assert_empty(fieldNames %-% self$dims_n_attrs,
                   "Invalid field(s) in select: [{.value}]")
      newMeta = private$metaList
      newMeta[['selected']] <- fieldNames
      private$create_new(private$raw_afl, meta_list = newMeta)
    }
    
    # db functions ----
    ,
    #' @description 
    #' Download query result of self's AFL string with all self's fields.
    #' 
    #' @return An R data frame with columns from self's dimensions and 
    #' attributes if no fields are selcted, or the selcted fields.
    to_df_all = function() {
      private$conn$query_all(private$to_df_afl())
    }
    ,
    #' @description 
    #' Download query result of self's AFL string with all self's attributes.
    #' 
    #' @return An R data frame with columns from self's attributes if no fields
    #' are selected, or the selcted fields only.
    to_df = function() {
      private$conn$query(private$to_df_afl(drop_dims = TRUE))
    }
    ,
    #' @description 
    #' Execute the AFL string for pure side effect without result returned.
    #' 
    #' @return self
    execute = function() {
      private$conn$execute(self$to_afl())
      invisible(NULL)
    }
    ,
    #' @description 
    #' Persist array operation as scidb array
    #' 
    #' If `self` is a persistent array and no `save_array_name` provided, then 
    #' `self` is returned. 
    #' 
    #' Otherwise, save self's AFL as a new scidb array. This includes two cases: 
    #'   1. `self` is persistent and `save_array_name` is provided, i.e. explicit persistence
    #'   2. `self` is array operation(s), then a new array is created regardless of `save_array_name`
    #'   
    #' From users perspective, 
    #' 
    #'  1. When we need to ensure a handle to a persistent array and do not care
    #'   whether it is a new or existing array, we should leave out 
    #'   `save_array_name` to avoid unnecessary array copying. E.g. `conn$array_from_df`
    #'   may return a build literal or uploaded persistent array, call
    #'   `conn$array_from_df(...)$persist()` to ensure a persistent array.
    #'  2. When we need to backup an array, then provide a `save_array_name` explicitly.
    #'  
    #'  Parameters `.gc` and `.temp` are only applicable when a new array is created.
    #' 
    #' @param save_array_name NULL or String. The new array name to save self's AFL as.
    #' If NULL, the array name is randomly generated when a new array is created.
    #' @param .temp Boolean, default FALSE. Whether to creaet a temporary scidb array.
    #' @param .gc Boolean, default TRUE. Whether to remove the persisted scidb array
    #' once the encapsulating arrayOp goes out of scodb in R. Set to FALSE if we
    #' need to keep the array indefinitely.
    #' @return A new arrayOp instance or self
    persist = function(
      save_array_name = NULL,
      .temp = FALSE,
      .gc = TRUE
    ){
      if(!is.null(save_array_name))
        assert_single_str(save_array_name)
      # No need to store an already persistent arrary
      if(self$is_persistent() && is.null(save_array_name)) return(self) 
      private$conn$private$array_from_stored_afl(
        private$to_df_afl(),
        save_array_name = save_array_name,
        .temp = .temp,
        .gc = .gc
      )
    }
    # array schema  ----
    ,
    #' @description 
    #' Create a new ArrayOp instance whose schema is the same as the `template`.
    #' 
    #' This operation throws away any fields that do not exist in `template` while keeping the `self`'s data of the 
    #' matching fields. 
    #' 
    #' Implemented by scidb `redimension` operator, but it allows for partial-fields match if `strict=F`.
    #' 
    #' @param template an ArrayOp instance as the schema template.
    #' @param strict If TRUE(default), requires `self` has all the `template` fields.
    #' @param .setting a string vector, where each item will be appended to the redimension operand. 
    #' E.g. .setting = c('false', 'cells_per_chunk: 1234') ==> redimension(source, template, false, cells_per_chunk: 1234)
    #' @return A new arrayOp instance 
    change_schema = function(template, strict = TRUE, .setting = NULL){
      realTemplate = if(strict) template
      else {
        matchingDims = template$dims %n% self$dims_n_attrs
        assert_has_len(matchingDims, 
                       "ERROR:ArrayOp$change_schema:None of the template dimension(s) '%s' found in the source.",
                       paste(template$dims, collapse = ','))
        matchedFileds = template$dims_n_attrs %n% self$dims_n_attrs
        template$spawn(excluded = template$dims_n_attrs %-% matchedFileds)
      }
      private$afl_redimension(realTemplate, .setting)
    }
    ,
    # @param .chunk_size aka. cells_per_chunk in 'flatten' mode.
    #' @description 
    #' Create a new arrayOp by dropping dimensions of 'self'.
    #' 
    #' Use `mode = 'unpack'` to still keep an artificial dimension in result arrayOp.
    #' The dimension is 0-based, auto-incremented up until `self$cell_count() - 1`.
    #' 'unpack' mode is useful in taking advantage of this artifical dimension to 
    #' auto populate other fields, e.g. in `set_auto_fields`.
    #' 
    #' Use `mode = 'flatten'` to return a scidb data frame which has no explicit dimensions.
    #' 
    #' Result arrayOp in both modes has attributes of self's attributes and dimensions.
    #' @param mode String 'unpack' (default) or 'flatten'.
    #' @param .chunk_size NULL or an integer. Converted to the 'chunk_size' param
    #' in 'unpack' mode; and 'cells_per_chunk' in 'flatten' mode.
    #' @param .unpack_dim NULL (default) or string as the dimension if 'unpack'
    #' mode is chosen. NULL defaults to a random field name.
    #' @return A new arrayOp instance
    drop_dims = function(mode = 'unpack', .chunk_size = NULL, .unpack_dim = dbutils$random_field_name()){
      assert_single_str(mode)
      if(!is.null(.chunk_size)) assert_single_num(.chunk_size)
      VALID_MODES = c('unpack', 'flatten')
      assertf(mode %in% VALID_MODES, 
              glue("unknown mode '{mode}'. Should be one of [{paste(VALID_MODES, collapse = ',')}]"))
      conn = private$conn
      if(mode == 'unpack'){
        .ifelse(
          is.null(.chunk_size),
          conn$afl_expr(afl(self | unpack(.unpack_dim))),
          conn$afl_expr(afl(self | unpack(.unpack_dim, .chunk_size)))
        )
      } else {
        .ifelse(
          is.null(.chunk_size),
          conn$afl_expr(afl(self | flatten)),
          conn$afl_expr(afl(self | flatten(glue("cells_per_chunk: {.chunk_size}"))))
        )
      }
    }
    ,
    #' @description 
    #' Create a new arrayOp with actual schema from SciDB or 'self' if 
    #' `self$is_schema_from_scidb == T`.
    #' 
    #' Useful in confirmming the schema of complex array operations.
    #' If the array schema is already retrieved from SciDB, then just return self.
    #' @return An arrayOp instance
    sync_schema = function() {
      if(self$is_schema_from_scidb) self else 
        if(!self$is_persistent())
          private$conn$afl_expr(self$to_afl()) else
            private$conn$array(self$to_afl())
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance using 'self' as a template
    #' 
    #' This function is mainly for array schema string generation when
    #' we want to rename, add, and/or exclude certain fields of self, but still 
    #' keep other unspecified fields unchanged.
    #'  
    #' Data types and dimension specs of existing fields are inherited from 'self' unless provided explicitly.
    #' New field data types default to NAs unless provided explicitly. 
    #' 
    #' This function is normally used internally for arrayOp generation.
    #' 
    #' @param afl_str An AFL expression. In case of using the spawned result as
    #' a schema template only, the afl_str does not need to be provided. Otherwise,
    #' it should conform with the actual resultant arrayOp instance, which is very rare.
    #' @param renamed A list of renamed fields where names are old fields and values are new field names.
    #' @param added New fields added to result arrayOp. String vector or NULL.
    #' @param excluded Fields excluded from `self`. String vector or NULL.
    #' @param dtypes NULL or a named list of data types for fields of the result arrayOp, 
    #' where names are field names, values (strings) are data types. 
    #' @param dim_specs NULL or a named list of array dimension specs,
    #' where names are dimension names, values (strings) are dimension specs in scidb format.
    #' @return A new arrayOp instance
    spawn = function(afl_str = "spawned array_op (as template only)",
                 renamed = NULL,
                 added = NULL,
                 excluded = NULL,
                 dtypes = NULL,
                 dim_specs = NULL) {
      assert_inherits(afl_str, "character")
      if(.not_empty(renamed)){
        assert_named_list(renamed, "ERROR: ArrayOp$spawn: 'renamed' param must be a named list, but got: %s", renamed)
      }
      assert_inherits(added,  c("NULL", "character"))
      assert_inherits(excluded,  c("NULL", "character"))
      
      attrs = self$attrs
      dims = self$dims
      oldDtypes = self$dtypes
      oldDimSpecs = private$get_dim_specs()
      
      # Rename the existing
      if(.not_empty(renamed)){
        renamedOldFields = names(renamed)
        assert_empty(renamedOldFields %-% self$dims_n_attrs, 
                     "Unknown field(s) in param `renamed`: [{.value}]")
        attrs = as.character(replace(attrs, attrs %in% renamedOldFields, .remove_null_values(renamed[attrs])))
        dims = as.character(replace(dims, dims %in% renamedOldFields, .remove_null_values(renamed[dims])))
        namesOldDtypes = names(oldDtypes)
        names(oldDtypes) <- replace(namesOldDtypes, namesOldDtypes %in% renamedOldFields, .remove_null_values(renamed[namesOldDtypes]))
        namesOldDimSpecs = names(oldDimSpecs)
        names(oldDimSpecs) <- replace(namesOldDimSpecs, namesOldDimSpecs %in% renamedOldFields, .remove_null_values(renamed[namesOldDimSpecs]))
      }
      
      # Add new fields
      if(.not_empty(added)){
        addedDims = added %n% names(dim_specs)
        addedAttrs = added %-% names(dim_specs)
        attrs = attrs %u% addedAttrs
        dims = dims %u% addedDims
      }
      
      # Exclude the excluded
      if(.not_empty(excluded)){
        attrs = attrs %-% excluded
        dims = dims %-% excluded
      }
      
      dtypes = .remove_null_values(
        c(dtypes, oldDtypes)[c(dims, attrs)]
      )
      
      dim_specs = .ifelse(.not_empty(dim_specs), c(dim_specs, oldDimSpecs), oldDimSpecs)
      
      private$create_new(afl_str, dims = dims, attrs = attrs, dtypes = dtypes, dim_specs = dim_specs)
    }
    # AFL -------------------------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' AFL string encapsulated by of the self ArrayOp
    #' 
    #' AFL can be either an scidb array name or array operation(s) on array(s).
    #' 
    #' The ArrayOp instance may have 'selected' fields but they are not reflected in the result.
    #' 'selected' fields are not reflected here, but determines which fields are retained in `to_df()` calls.
    #' 
    #' @return an AFL expression string
    to_afl = function() {
      return(private$raw_afl)
    }
    ,
    #' @description 
    #' Return a schema representation of the ArrayOp `<attr1:type1 [, attr2:type2 ...]> [dim1 [;dim2]]`
    #' 
    #' Unless `sync_schema()` is called, the schema may be inferred locally in R to save round trips between R and SciDB server. 
    #' SciDB data frames have hidden dimensions that start with `$`
    #' 
    #' @return SciDB schema string
    to_schema_str = function() {
      attrStr = paste(self$attrs, private$get_field_types(self$attrs), sep = ':', collapse = ',')
      # SciDB data frame format
      if(length(self$dims) == 0) return(sprintf("<%s>", attrStr))
      
      dimStrItems = mapply(function(dimName, dimSpec){
        if(.not_empty(dimSpec) && nchar(dimSpec) > 0) sprintf("%s=%s", dimName, dimSpec)
        else dimName
      }, self$dims, private$get_dim_specs())
      dimStr = paste(dimStrItems, collapse = ';')
      sprintf("<%s> [%s]", attrStr, dimStr)
    }
    # Misc array operators ------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Create a new arrayOp that encapsulate AFL for the first `n` cells of 'self'
    #' 
    #' We still need to append a `to_df` call to download the result as data frame.
    #' @param n How many cells to take
    #' @param skip How many rows to skip before taking
    #' @return A new ArrayOp instance
    limit = function(n = 5, skip = NULL) {
      assert_single_number(n)
      assert(is.null(skip) || is.numeric(skip))
      aflStr = if(is.null(skip)) afl(self | limit(n)) else afl(self | limit(n, skip))
      self$spawn(aflStr)
    }
    ,
    #' @description 
    #' Return the number of cells of 'self'
    #' @return A number of cells if the AFL that 'self' encapsulates is run.
    cell_count = function(){
      if(self$is_persistent()) self$summarize_array()$count
      else private$conn$query(afl(self | op_count))[["count"]]
    }
    ,
    #' @description 
    #' Return a data frame of the summary of the 'self' array
    #' 
    #' Implemented by scidb 'summarize' operator
    #' @param by_attribute Summarize by array attributes
    #' @param by_instance Summarize by array scidb instances
    #' @param return A data frame of the 'self' array summary 
    summarize_array = function(by_attribute = FALSE, by_instance = FALSE){
      private$conn$query_all(afl(self | summarize(
        glue("by_attribute:{by_attribute}, by_instance:{by_instance}")))
      )
    }
    ,
    #' @description 
    #' Return a data frame of all self's versions
    #' @return An R data frame with columns: version_id and timestamp
    list_versions = function(){
      assertf(self$is_persistent(), "list_versions only works on persistent arrays")
      private$conn$query(afl(self | versions))
    }
    ,
    #' @description 
    #' Get an arrayOp instance that encapsulates a version snapshot of a persistent 
    #' scidb array
    #' 
    #' The function does not perform version check in scidb. It only construct
    #' an arrayOp locally to represent a specific version. If a non-existent
    #' version_id is later used in scidb related operations, an error will be 
    #' thrown by SciDB.
    #' 
    #' @param version_id A number of the array version_id
    #' @return An arrayOp instance with the same schema as self
    version = function(version_id) {
      assertf(self$is_persistent(), "version only works on persistent arrays")
      assert_single_num(version_id)
      self$spawn(sprintf("%s@%s", self$to_afl(), version_id))
    }
    ,
    #' @description 
    #' Returns whether the current arraOp instance encapsulates a persistent scidb
    #' array namne that may or may not exist on the scidb server
    #' 
    #' No checking with scidb server is performed. Only validate the arrayOp's AFL 
    #' with regex and see if it matches an array name. E.g. "myNamespace.myArray"
    #' or "myArrayInPublicNamespace". 
    #' 
    #' @return TRUE or FALSE
    is_persistent = function(){
      !grepl("\\(", self$to_afl()) && 
        grepl("^((\\w+)\\.)?(\\w+)$", self$to_afl())
    }  
    ,
    #' @description 
    #' Returns whether the current arraOp instance encapsulates a persistent scidb
    #' array that exists on the scidb server
    #' 
    #' If current arrayOp encapsulates an array operation, then it returns FALSE
    #' without checking with scidb server.
    #' 
    #' @return TRUE or FALSE
    exists_persistent_array = function() {
      self$is_persistent() && nrow(self$array_meta_data()) == 1L
    }
    ,
    #' @description 
    #' Download the array meta as an R data frame 
    #' 
    #' The array metadata is retrieved from executing the scidb 'show' operator
    #' in the array namespace and match for the current array name.
    #' Array metadata include fields: "name", "uaid", "aid", "schema", "availability", "temporary",  "namespace", "distribution", "etcomp"
    #' @return An R data frame
    array_meta_data = function(){
      assertf(self$is_persistent(), 
              "Array meta data is only available for persistent scidb arrays.")
      full_array_name = self$to_afl()
      ns = gsub("^((\\w+)\\.)?(\\w+)$", "\\2", full_array_name)
      bare_name = gsub("^((\\w+)\\.)?(\\w+)$", "\\3", full_array_name)
      query_str = if(ns == "") "list('arrays')" else 
        sprintf("list('arrays', ns:%s)", ns)
      private$conn$query(sprintf("filter(%s, name = '%s')", query_str, bare_name))
    }
    ,
    #' @description 
    #' Remove array versions of self
    #' 
    #' Only applicable to persistent arrays.
    #' **Warning**: This function will be executed effectively in scidb without
    #' extra 'execute()' and cannot be undone.
    #' @param version_id NULL or a number. When set to NULL, all array versions 
    #' are removed except for the latest one. When set to an number, must be 
    #' a valid version_id of self, in which case all versions up to the 'version_id'
    #' are removed.
    #' @return NULL
    remove_versions = function(version_id = NULL){
      assertf(self$is_persistent(), "remove_versions only works on persistent arrays")
      if(is.null(version_id)) {
        private$conn$execute(afl(self | remove_versions))
      } else {
        version_id = as.character(version_id)
        assert_single_str(version_id, "ERROR: param 'version_id' cannot be converted to a single str: %s", deparse(version_id))
        private$conn$execute(afl(self | remove_versions(version_id)))
      }
      invisible(NULL)
    }
    ,
    #' @description 
    #' Remove array versions of self
    #' 
    #' Only applicable to persistent arrays.
    #' **Warning**: This function will be executed effectively in scidb without
    #' extra 'execute()' and cannot be undone.
    #' @return NULL
    remove_array = function(){
      assertf(self$is_persistent(), "remove_array only works on persistent arrays")
      private$conn$execute(afl(self | remove))
      invisible(NULL)
    }
    ,
    #' @description 
    #' A finalize function executed when the 'self' instance is garbage collected in R
    #' 
    #' If an arrayOp is marked as .gc = T, then it will be removed from scidb
    #' when this function is executed. 
    #' 
    #' We don't normally call this function except in testing.
    finalize = function() {
      refKey = '.refs'
      # private$metaList[[refKey]] = NULL
      # private$metaList = NULL
      refObj = private$get_meta(refKey)
      if(!is.null(refObj)) {
        
        # Try to remove the scidbR object if its obj@meta$remove == TRUE
        removeSciDbRObj = function(obj) {
          # printf("cleaning up: %s [gc=%s]\n%s", obj@name, obj@meta$remove, obj@meta$schema)
          if(!is.null(obj@meta$remove) && obj@meta$remove){
            # printf("remove(%s)", obj@name)
            obj@meta$remove <- F
            try(scidb::iquery(obj@meta$db, sprintf("remove(%s)", obj@name)), silent = T)
          }
        }

        cleanUpObj = function(obj) {
          if("scidb" %in% class(obj)) removeSciDbRObj(obj)
          # recursivlly call this function if another arrayOp is involved
          else if(inherits(obj, "ArrayOpBase")) obj$finalize()
        }

        # printf("finialize arrayop: %s", self$to_afl())
        if(length(refObj) > 1) sapply(refObj, cleanUpObj)
        else cleanUpObj(refObj[[1]])

        private$set_meta(refKey, NULL)
      }
    }
  )
)

