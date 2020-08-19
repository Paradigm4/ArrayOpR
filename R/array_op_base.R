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

#' Base class of all ArrayOp classes
#' @description 
#' ArrayOp classes denote scidb array operations and operands, hence the name. 
#' @details 
#' One operation consists of an scidb operator and [1..*] operands, of which the result can be used as an operand 
#' in another operation. Operands and Opreration results can all be denoted by ArrayOp.
#' @export
ArrayOpBase <- R6::R6Class(
  "ArrayOpBase",
  cloneable = FALSE,                         
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
    conn = NULL # The ScidbConnection instance
    ,
    # todo: move connection setup to initialize function
    set_conn = function(connection) {
      private$conn = connection
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
      assert(inherits(operand, class(self)),
        "JoinOp arg '%s' must be class of [%s], but got '%s' instead.",
        side, 'ArrayOpBase', class(operand))
      assert(is.character(keys) && .has_len(keys),
        "Join arg 'on_%s' must be a non-empty R character, but got '%s' instead", side, class(keys))
      absentKeys = operand$get_absent_fields(keys)
      assert_not_has_len(absentKeys, "JoinOp arg 'on_%s' has invalid fields: %s", side, paste(absentKeys, collapse = ', '))
    }
    ,
    validate_join_params = function(left, right, on_left, on_right, settings) {
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
    #' @return An AFL string
    to_join_operand_afl = function(keyFields, keep_dimensions = FALSE, artificial_field = utility$random_field_name()) {
      
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
        res = afl(arrName | apply(artificial_field, 'null') | project(artificial_field))
      }
      # case-2: Regular mode. Just 'apply'/'project' for dimensions/attributes when needed.
      else{
        applyExpr = if(.has_len(applyList))
          afl(arrName | apply(afl_join_fields(applyList, applyList)))
        else arrName
        res =  if(.has_len(projectList))
          afl(applyExpr | project(projectList))
        else applyExpr
      }
      return(res)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance by joining with another ArrayOp 
    #' 
    #' Currently implemented with scidb `equi_join` operator.
    #' @param right The other ArrayOp instance to join with.
    #' @param on_left R character vector. Join keys from the left (self).
    #' @param on_right R character vector. Join keys from the `right`. Must be of the same length as `on_left`
    #' @param on_both Join keys on both operand.
    #' @param .auto_select default: FALSE. If set to TRUE, the resultant ArrayOp instance will auto `select` the fields
    #' that are selected in the left and right operands but not in the right join keys (since they are masked by equi_join operator).
    #' @param settings `equi_join` settings, a named list where both key and values are strings. 
    #' @param .dim_mode How to reshape the resultant ArrayOp. Same meaning as in `ArrayOp$reshape` function. 
    #' By default, dim_mode = 'keep', the artificial dimensions, namely `instance_id` and `value_no` from `equi_join`
    #' are retained. If set to 'drop', the artificial dimensions will be removed. See `ArrayOp$reshape` for more details.
    #' @param .artificial_field As in `ArrayOp$reshape`, it defaults to a random field name. It can be safely ignored in
    #' client code. It exists only for test purposes. 
    #' @return A new arrayOp 
    join = function(left, right, 
                    on_left = NULL, on_right = NULL, on_both = NULL, 
                    .auto_select = FALSE, join_mode = 'equi_join',
                    settings = NULL, 
                    .left_alias = '_L', .right_alias = '_R') {
      if(.has_len(on_both)){
        on_left = on_both %u% on_left
        on_right = on_both %u% on_right
      }
      switch(join_mode,
             'equi_join' = private$equi_join,
             'cross_join' = private$cross_join,
             stopf("ERROR: ArrayOp$join: Invalid param 'join_mode' %s. Must be one of [equi_join, cross_join, auto]", join_mode)
      )(
        left$.private$auto_select(), 
        right$.private$auto_select(), 
        on_left, on_right, settings = settings, 
        .auto_select = .auto_select, 
        .left_alias = .left_alias, .right_alias = .right_alias)
    }
    ,
    equi_join = function(left, right, on_left, on_right, settings = NULL, .auto_select = FALSE, 
      .left_alias = '_L', .right_alias = '_R') {
      # Validate join params
      private$validate_join_params(left, right, on_left, on_right, settings)
      
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
      settingItems = mapply(function(k, v) private$to_equi_join_setting_item_str(k, v, .left_alias, .right_alias), 
        names(mergedSettings), mergedSettings)

      keep_dimensions = (function(){
        val = settings[['keep_dimensions']]
        .has_len(val) && val == 1
      })()
      
      # Join two operands
      joinExpr <- sprintf(private$equi_join_template(.left_alias, .right_alias),
        left$.private$to_join_operand_afl(on_left, keep_dimensions = keep_dimensions), 
        right$.private$to_join_operand_afl(on_right, keep_dimensions = keep_dimensions),
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
      dtypes = .remove_null_values(c(dims, c(left$dtypes, right$dtypes)[attrs]))
      
      if(hasSelected){
        selectedFields = c(left$selected, right$selected %-% on_right)
        duplicatedFields = selectedFields[duplicated(selectedFields)]
        selectedFields = if(length(duplicatedFields) > 0L){

          selectedLeftFields = Map(function(x) if(x %in% duplicatedFields) sprintf("%s.%s", .left_alias, x) else x, 
                                   left$selected)
          selectedRightFields = Map(function(x) if(x %in% duplicatedFields) sprintf("%s.%s", .right_alias, x) else x, 
                                   right$selected %-% on_right)
          selectedLeftNames= Map(function(x) if(x %in% duplicatedFields) sprintf("%s%s", x, .left_alias) else x, 
                                   left$selected)
          selectedRightNames = Map(function(x) if(x %in% duplicatedFields) sprintf("%s%s", x, .right_alias) else x, 
                                   right$selected %-% on_right)
          new_named_list(
            values = selectedLeftFields %u% selectedRightFields,
            names = selectedLeftNames %u% selectedRightNames
          )
        } else new_named_list(selectedFields, selectedFields)
        joinedOp = self$create_new(joinExpr, names(dims), attrs, dtypes = dtypes)
        return(
          joinedOp$.private$reshape_attrs(selectedFields)$select(names(selectedFields))
        )
        # return(do.call(joinedOp$transmute, selectedFields)$select(names(selectedFields)))
      }
      
      selectedFields = if(hasSelected) attrs else NULL
      joinedOp = self$create_new(joinExpr, names(dims), attrs, dtypes = dtypes)
      if(hasSelected) {
        joinedOp = joinedOp$reshape(select = selectedFields)
        if(.auto_select)
          joinedOp = joinedOp$select(selectedFields)
      }
      joinedOp
    }
    ,
    cross_join = function(left, right, on_left, on_right, settings = NULL, .auto_select = FALSE, 
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
      private$validate_join_params(left, right, on_left, on_right, settings)
      assert_keys_are_all_dimensions('left', left, on_left)
      assert_keys_are_all_dimensions('right', right, on_right)
      # Construct the AFL
      joinDims = paste(sprintf(", %s.%s, %s.%s", .left_alias, on_left, .right_alias, on_right), collapse = '')
      aflStr = sprintf("cross_join(%s as %s, %s as %s %s)", left$to_afl(), .left_alias, right$to_afl(), .right_alias, joinDims)
      attrs = c(left$attrs, right$attrs)
      dims = c(left$dims, right$dims %-% on_right)
      dtypes = c(left$get_field_types(.strict = F), right$get_field_types(right$dims_n_attrs %-% on_right, .strict = F))
      dim_specs = c(left$get_dim_specs(), right$get_dim_specs(right$dims %-% on_right))
      self$create_new(aflStr, dims = dims, attrs = attrs, dtypes = dtypes, dim_specs = dim_specs)
    }
    ,
    # Reshape an array without modifying its dimensions
    # 
    # This is an enhanced version inspired by scidb 'project' and 'apply' operators which also only work on attributes.
    reshape_attrs = function(select=NULL, dtypes = NULL, artificial_field = .random_attr_name(), .force_project = TRUE) {
      assert(.has_len(select),
             "ERROR: ArrayOp$reshape_attrs: param 'select' must be a non-empty character list, but got: %s", 
             class(select))
      
      # Plain selected fields without change
      existingFields = if(.has_len(names(select))) {
        as.character(select[names(select) == '' | names(select) == select])
      } else {
        as.character(select)
      }
      # Names of the retained fields (existing or new)
      selectFieldNames = .get_element_names(select)
      fieldExprs = select[names(select) != '']
      fieldNamesWithExprs = names(fieldExprs)
      
      newFieldNames = fieldNamesWithExprs %-% self$attrs
      newFields = fieldExprs[newFieldNames]
      
      replacedFieldNames = fieldNamesWithExprs %-% newFieldNames %-% existingFields
      replacedFieldNamesAlt = sprintf("_%s", replacedFieldNames)
      replacedFields = sapply(replacedFieldNames, function(x) gsub('@', x, fieldExprs[[x]]))
      
      mergedDtypes = utils::modifyList(self$dtypes, as.list(dtypes))
      
      attrs = selectFieldNames %-% self$dims
      
      newAfl = if(.has_len(replacedFieldNames)){
        afl(self | apply(afl_join_fields(replacedFieldNamesAlt, replacedFields))
            | project(attrs %-% newFieldNames %-% replacedFieldNames %u% replacedFieldNamesAlt) 
            | apply(afl_join_fields(replacedFieldNames %u% newFieldNames, replacedFieldNamesAlt %u% newFields))
            | project(attrs)
            )
      }
      else if(.has_len(attrs)) {
        inner = self
        if(.has_len(newFieldNames))
          inner = afl(self | apply(afl_join_fields(newFieldNames, newFields)))
        if(!.force_project && length(attrs) == length(self$attrs) && all(attrs == self$attrs))
          afl(inner)
        else
          afl(inner | project(attrs))
      }
      else {
        attrs = artificial_field
        mergedDtypes[[artificial_field]] = 'void'
        afl(self | apply(c(artificial_field, 'null')) | project(artificial_field))
      }
      self$create_new(newAfl, self$dims, attrs, mergedDtypes, dim_specs = self$get_dim_specs(),
                      validate_fields = private$get_meta('validate_fields'))
      
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
        return(operand$reshape(reserved_fields))
      
      operandKeyFields = .get_element_names(keys)
      templateKeyFields = as.character(keys)
      extraFields = operand$attrs %-% keys %-% reserved_fields
      
      if(all(self$dims %in% operandKeyFields)){
        if(.has_len(extraFields))
          operand = operand$reshape(c(keys, reserved_fields))
        return(operand$change_schema(self, strict = FALSE, .setting = .redimension_setting)$
                 reshape(reserved_fields, .force_project = FALSE))
      }
      
      joinOp = private$join(
        operand$select(reserved_fields %u% (self$dims %n% operand$dims_n_attrs)),
        self$select(self$dims), 
        on_left = operandKeyFields, 
        on_right = templateKeyFields, 
        settings = .join_setting)
      joinOp$change_schema(self, strict = FALSE, .setting = .redimension_setting)
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
    #' @return A new arrayOp 
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
                         paste(ref_field %-% reference$dims_n_attrs, ','))
      
      refDims = ref_field %-% reference$attrs
      maxRefFields = sprintf("_max_%s", ref_field)
      aggFields = sprintf("max(%s) as %s", ref_field, maxRefFields)
      defaultOffset = ref_start - source_start
      nonDefaultOffset = 1 - source_start
      
      if(!.has_len(new_field)) new_field = ref_field
      newFieldExpr = sprintf("iif(%s is null, %s%s, %s+%s%s)", maxRefFields, 
                             source_field, .to_signed_integer_str(defaultOffset), 
                             maxRefFields, source_field, .to_signed_integer_str(nonDefaultOffset))
      
      forAggregate = if(.has_len(refDims)) afl(reference | apply(afl_join_fields(refDims, refDims))) else reference
      aggregated = afl(forAggregate | aggregate(aggFields))
      crossJoined = afl(
        self |
          cross_join(aggregated) | 
          apply( 
            afl_join_fields(new_field, newFieldExpr)
          )
      )
      result = self$spawn(added = new_field, dtypes = rlang::set_names(reference$get_field_types(ref_field, .strict = FALSE), new_field))
      result = result$create_new_with_same_schema(crossJoined)
      result = result$reshape(result$attrs)
      result
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance that has an anti-collision field set according to a template arrayOp
    #' 
    #' The source (self) operand should have N fields given the target has N+1 dimensions. The one missing field is 
    #' treated as the anti-collision field.
    #'
    #' @param target A target arrayOp that the source draws anti-collision dimension from.
    #' @param anti_collision_field a target dimension name which exsits only to resolve cell collision 
    #' (ie. cells with the same dimension coordinate).
    #' @return A new arrayOp 
    set_anti_collision_field = function(target, anti_collision_field = NULL, join_setting = NULL, source_anti_collision_dim_spec = NULL) {
      assert(inherits(target, 'ArrayOpBase'),
             "ERROR: ArrayOp$set_anti_collision_field: param target must be ArrayOp, but got '%s' instead.", class(target))
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
          renamedTarget$get_dim_specs()
      else
        utils::modifyList(renamedTarget$get_dim_specs(),
                          as.list(structure(source_anti_collision_dim_spec, names = srcAltId)))
      redimensionTemplate = self$create_new("TEMPLATE", 
                                            dims = renamedTarget$dims, 
                                            attrs = self$attrs %-% renamedTarget$dims,
                                            dtypes = utils::modifyList(self$get_field_types(), renamedTarget$get_field_types(renamedTarget$dims)),
                                            dim_specs = redimensionTemplateDimSpecs)
      redimenedSource = redimensionTemplate$create_new_with_same_schema(afl(
        self | redimension(redimensionTemplate$to_schema_str()) | apply(srcAltId, srcAltId)
      ))
      
      # Get the max anti-collision-field from group aggregating the target on the remainder of target dimensions
      targetAltIdMax = sprintf("max_%s_", anti_collision_field)
      groupedTarget = self$create_new('',
        attrs = c(regularTargetDims, targetAltIdMax), dims = c('instance_id', 'value_no'),
        dtypes = c(invert.list(list("int64" = c('instance_id', 'value_no', regularTargetDims, targetAltIdMax))))
      )$create_new_with_same_schema(afl(
        target | apply(anti_collision_field, anti_collision_field) | grouped_aggregate(
          sprintf("max(%s) as %s", anti_collision_field, targetAltIdMax), regularTargetDims)
      ))
        
      # Left join on the remainder of the target dimensions
      joinSetting = if(is.null(join_setting)) list(left_outer=1,keep_dimensions=TRUE) else {
        utils::modifyList(as.list(join_setting), list(left_outer=1, keep_dimensions=TRUE))
      }
      joined = private$join(redimenedSource$select(redimenedSource$dims_n_attrs), 
                            groupedTarget$select(targetAltIdMax), 
                            on_both = regularTargetDims,
                            .auto_select = TRUE,
                            settings = joinSetting)
      
      # Finally calculate the values of anti_collision_field
      # src's attributes (that are target dimensions) are converted to dimensions according to the target
      resultTemplate = redimensionTemplate$
        spawn(renamed = invert.list(renamedList))
      resultTemplate = resultTemplate$reshape(dim_mode = 'drop')
      
      
      result = resultTemplate$
        create_new_with_same_schema(afl(
          joined | apply(anti_collision_field, sprintf(
            "iif(%s is null, %s, %s + %s + 1)", targetAltIdMax, srcAltId, srcAltId, targetAltIdMax
          )))
        )$reshape(resultTemplate$attrs)
      
      return(result)
    }
    ,
    #' @description 
    #' Return AFL suitable for retrieving data.frame.
    #'
    #' scidb::iquery has a param `only_attributes`, which, if set TRUE, will effectively drop all dims.
    #' @param drop_dims Whether self's dimensions are dropped when generating AFL for data.frame conversion
    #' @return An AFL string
    to_df_afl = function(drop_dims = FALSE, artificial_field = .random_attr_name()) {
      return(private$to_afl_explicit(drop_dims, self$selected, artificial_field = artificial_field))
    }
    ,
    #' @description 
    #' Returns AFL when self used as an operand in another parent operation.
    #'
    #' By default, 1. dimensions are not dropped in parent operation; 2. no intent to select fields
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' @param selected_fields which fields are selected no matter what the parent operation is.
    #' If NULL, self fields will pass on by default depending on the parent operation.
    #' @return An AFL string
    to_afl_explicit = function(drop_dims = FALSE, selected_fields = NULL, artificial_field = .random_attr_name()){
      
      # No intent to select fields
      if(!.has_len(selected_fields))
        return(self$to_afl())
      
      # 'selected_fields' is specified in sub-classes.
      # We need to ensure selected fields are passed on in parent operations.
      
      # dims are dropped in parent operations.
      if(drop_dims){
        selectedDims = base::intersect(selected_fields, self$dims)
        inner = if(.has_len(selectedDims))
          afl(self | apply(afl_join_fields(selectedDims, selectedDims)))
        else self$to_afl()
        return(afl(inner | project(afl_join_fields(selected_fields))))
      }
      
      # drop_dims = F. All dims are preserved in parent operations, we can only select/drop attrs.
      selectedAttrs = base::intersect(selected_fields, self$attrs)
      if(.has_len(selectedAttrs))  # If some attributes selected
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
    # Return a result array instance with the same dimensions and a subset (projected) attributes
    # Throw an error if there are non-attribute fileds because scidb only allows project'ing on attributes
    afl_project = function(...) {
      fields = c(....)
      nonAttrs = fields %-% self$attrs
      assert_not_has_len(nonAttrs, "ERROR: afl_project: %d non-attribute field(s) found: %s", length(nonAttrs), paste(nonAttrs, collapse = ', '))
      if(!.has_len(fields)) return(self)
      self$create_new(afl(self | project(fields)), dims = self$dims, attrs = fields, 
                      dtypes = self$get_field_types(c(self$dims, fields)), dim_specs = self$get_dim_specs())
    }
    ,
    # Apply new attributes to an existing array. 
    # Return a result array with added (applied) attributes
    # If fields are existing dimensions, data types are inheritted; otherwise new attributes require data types
    # @param fields: a named list or character. Cannot contain existing attributes because it creates conflicts.
    afl_apply = function(fields, dtypes = NULL) {
      fieldNames = .get_element_names(fields)
      fieldExprs = as.character(fields)
      conflictFields = fields %n% self$attrs
      assert_not_has_len(conflictFields, "ERROR: afl_apply: cannot apply existing attribute(s): %s", paste(conflictFields, collapse = ', '))
      
      newDTypes = utils::modifyList(self$get_field_types(), as.list(dtypes))
      self$create_new(afl(self | apply(afl_join_fields(fieldNames, fieldExprs))), dims = self$dims, 
                      attrs = self$attrs %u% fields, dtypes = newDTypes, dim_specs = self$get_dim_specs())
    }
    ,
    # Redimension `self` according to a template.
    # 
    # @param .setting a string vector, where each item will be appended to the redimension operand.
    # E.g. .setting = c('false', 'cells_per_chunk: 1234') ==> redimension(source, template, false, cells_per_chunk: 1234)
    # Similar to scidb redimension operator
    afl_redimension = function(template, .setting = NULL) {
      assert_no_fields(template$dims_n_attrs %-% self$dims_n_attrs, 
                       "ERROR:ArrayOp$afl_redimension:Field(s) '%s' of the template not found in the source.")
      return(template$create_new_with_same_schema(afl(
        self | redimension(c(template$to_schema_str(), as.character(.setting)))
      )))
    }
    ,
    # Insert to a target array (scidb `insert` operator)
    # 
    # Perform field data types check. Field names are irrelevant
    # 
    # @param target: Target array
    # @return An scidb insert operation
    afl_insert = function(target) {
      assert(length(self$dims) == length(target$dims), "ERROR: ArrayOp$afl_insert: dimension number mismatch: %d[%s] != %d[%s]",
             length(self$dims), paste(self$dims, collapse=','), length(target$dims), paste(target$dims, collapse = ','))
      assert(length(self$attrs) == length(target$attrs), "ERROR: ArrayOp$afl_insert: attribute number mismatch: %d[%s] != %d[%s]",
             length(self$attrs), paste(self$attrs, collapse = ','), length(target$attrs), paste(target$attrs, collapse = ','))
      assert(all(as.character(self$get_field_types(.raw = TRUE)) == as.character(target$get_field_types(.raw = TRUE))),
             "ERROR: ArrayOp$afl_insert: attribute data type mismatch. \nSource: %s\nTarget: %s", 
             self$to_schema_str(), target$to_schema_str())
      target$create_new_with_same_schema(afl(self | insert(target)))
    }
    ,
    # Overwrite a target array (scidb `store` operator)
    # 
    # Perform field data types check. Field names are irrelevant
    # 
    # @param target: Target array
    # @return An scidb insert operation
    afl_store = function(target, new_target = FALSE) {
      assert(length(self$dims) == length(target$dims), "ERROR: ArrayOp$afl_store: dimension number mismatch: %d[%s] != %d[%s]",
             length(self$dims), paste(self$dims, collapse=','), length(target$dims), paste(target$dims, collapse = ','))
      assert(length(self$attrs) == length(target$attrs), "ERROR: ArrayOp$afl_store: attribute number mismatch: %d[%s] != %d[%s]",
             length(self$attrs), paste(self$attrs, collapse = ','), length(target$attrs), paste(target$attrs, collapse = ','))
      assert(all(as.character(self$get_field_types(.raw = TRUE)) == as.character(target$get_field_types(.raw = TRUE))),
             "ERROR: ArrayOp$afl_store: attribute data type mismatch. \nSource: %s\nTarget: %s", 
             self$to_schema_str(), target$to_schema_str())
      target$create_new_with_same_schema(afl(self | store(target)))
    }
  ),
  active = list(
    #' @field dims Dimension names
    dims = function() private$get_meta('dims'),
    #' @field attrs Attribute names
    attrs = function() private$get_meta('attrs'),
    #' @field selected Selected dimension and/or attribute names
    selected = function() private$get_meta('selected'),
    #' @field dtypes A named list, where key is dim/attr name and value is respective SciDB data type as string
    dtypes = function() private$get_meta('dtypes'),
    #' @field dims_n_attrs Dimension and attribute names
    dims_n_attrs = function() c(self$dims, self$attrs),
    #' @field attrs_n_dims Attribute and dimension names
    attrs_n_dims = function() c(self$attrs, self$dims),
    #' @field .private For internal testing only. Do not access this field to avoid unintended consequences!!!
    .private = function() private
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
    #' @param field_names R character. If NULL, defaults to `self$dims_n_attrs`, ie. dimensions and attributes.
    #' @param .raw Default FALSE, full data types are returned; if set TRUE, only the raw data types are returned 
    #' (raw data types are string, int32, int64, bool, etc, without scidb attribute specs such as: string compression 'zlib')
    #' @return a named list as `field_names`, where absent fields or fields without data types will cause error; 
    #' unless `.strict=F`, absent fields are ignored
    get_field_types = function(field_names = NULL, .strict = TRUE, .raw = FALSE){
      if(is.null(field_names))
        field_names = self$dims_n_attrs
      missingFields = base::setdiff(field_names, self$dims_n_attrs)
      assert_not_has_len(missingFields, 
        "ERROR: ArrayOp$get_field_types: field_names: Field(s) '%s' not found in ArrayOp: %s", 
        paste(missingFields, collapse = ','),
        private$raw_afl
      )
      if(.strict){
        missingFields = field_names %-% names(self$dtypes)
        assert_not_has_len(missingFields, 
          "ERROR: ArrayOp$get_field_types: Field(s) '%s' not annotated with dtype in ArrayOp: %s", 
          paste(missingFields, collapse = ','),
          private$raw_afl
        )
      }
      result = self$dtypes[field_names]
      if(.raw){
        result = as.list(structure(regmatches(result, regexpr("^\\w+", result)), names = field_names))
      }
      result
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
    #' @return A list of absent field names
    get_absent_fields = function(fieldNames) {
      if(!private$get_meta('validate_fields'))
        return(NULL)
      absentMarks = !(fieldNames %in% self$dims_n_attrs)
      return(fieldNames[absentMarks])
    }
    # Functions that creat new ArrayOps -------------------------------------------------------------------------------
    ,
    #' @description 
    #' Return a new arrayOp instance with the same version as `self`
    #' 
    #' Work in sub-class without requiring class names or constructor function.
    #' @param ... The samme params with Repo$ArrayOp(...)
    #' @return A new arrayOp 
    create_new = function(...){
      classConstructor = get(class(self))$new
      result = classConstructor(...)
      result$.private$set_conn(private$conn)
      result
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance of the same class
    #' 
    #' The new instance shares all meta data with the template
    #' @param new_afl AFL for the new ArrayOp
    #' @param ... Named params in `...` will replace the items in the template's metaList
    #' @return A new arrayOp 
    create_new_with_same_schema = function(new_afl, ...) {
      metaList = utils::modifyList(private$metaList, list(...))
      self$create_new(new_afl, metaList = metaList)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance by using a filter expression on the parent ArrayOp
    #' 
    #' Similar to SQL where clause and dplyr::filter.
    #' @param missing_fields_error_template Error template for missing fields. 
    #' Only one %s is allowed which is substituted with an concatnation of the missing fields separated by commas.
    #' @param regex_func A string of regex function implementation. 
    #' Due to scidb compatiblity issue with its dependencies, the regex function from boost library may not be available
    #' Supported values: rsub, regex
    #' @param ignore_case A Boolean. If TRUE, ignore case in string match patterns. 
    #' Otherwise, perform case-sensitive regex matches.
    #' @return A new arrayOp 
    filter = function(..., expr, missing_fields_error_template = NULL, 
                     regex_func = getOption('arrayop.regex_func', default = 'regex'), 
                     ignore_case = getOption('arrayop.ignore_case', default = TRUE)) {
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
      newRawAfl = if(.has_len(filterExpr)) 
        afl(self | filter(afl_filter_from_expr(filterExpr, regex_func = regex_func, ignore_case = ignore_case)))
      else self$to_afl()
      self$create_new_with_same_schema(newRawAfl)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with selected fields
    #' 
    #' NOTE: this does NOT change the to_afl output, but explicitly state which field(s) are retained if used in
    #' a parent operation that changes its schema, e.g. equi_join or to_df(only_attributes = T)
    #' @param ... Which field(s) are retained during a schema-change operation
    #' @return A new arrayOp 
    select = function(...) {
      fieldNames = c(...)
      assert(is.character(fieldNames) || is.null(fieldNames), 
        "ERROR: ArrayOp$select: ... must be a character or NULL, but got: %s", fieldNames)
      # private$assert_fields_exist(fieldNames, "ArrayOp$select")
      assert_no_fields(fieldNames %-% self$dims_n_attrs, "ERROR: ArrayOp$select: invalid select fields [%s] in: %%s", self$to_afl())
      newMeta = private$metaList
      newMeta[['selected']] <- fieldNames
      self$create_new(private$raw_afl, metaList = newMeta)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance with a different schema/shape
    #' 
    #' @param select Which attributes to select or create.
    #' In dim_mode='keep', `select` must be a non-empty list, where named items are derived new attributes and
    #' unamed string values are existing dimensions/attributes. Dimensions, selected or not, are all retained in
    #' dim_mode='keep'.
    #' In dim_mode='drop', `select` can be NULL, which effectively select all source dimensions and attributes.
    #' Unselected dimensions will be discarded. 
    #' @param dtypes a named list to provide field data types for newly derived fields
    #' @param dim_mode a string [keep, drop]. 
    #' In the default 'keep' mode, only attributes can be selected. All dimensions are kept.
    #' In 'drop' mode, dimensions are first converted to attributes, then selected.  
    #' @param artificial_field ONLY relevant when `dim_mode='drop'`.
    #' A field name used as the artificial dimension name in 'drop' dim_mode 
    #' (internally used by `unpack` scidb operator). By default, a random string is generated.
    #' @return A new arrayOp 
    reshape = function(select=NULL, dtypes = NULL, dim_mode = 'keep', artificial_field = .random_attr_name(), 
                       .force_project = TRUE) {
      
      keep = function(){
        private$reshape_attrs(select, dtypes, artificial_field, .force_project = .force_project)
      }
      
      drop = function() {
        unpacked = self$create_new(
          afl(self | unpack(artificial_field)), 
          attrs = self$dims_n_attrs, dims = artificial_field,
          dtypes = utils::modifyList(self$get_field_types(), as.list(rlang::set_names('int64', artificial_field)))
        )
        if(!.has_len(select))
          unpacked
        else 
          unpacked$.private$reshape_attrs(select, dtypes, artificial_field)
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
    #' Change `self` schema according to a template
    #' 
    #' This operation throws away any fields that do not exist in `template` while keeping the `self`'s data of the 
    #' matching fields. Implemented by scidb `redimension` operator, but it allows for partial-fields match if `strict=F`.
    #' 
    #' @param template an arrayOp instance that determines the resultant schema.
    #' @param strict If TRUE(default), requires `self` has all the `template` fields.
    #' @param .setting a string vector, where each item will be appended to the redimension operand. 
    #' E.g. .setting = c('false', 'cells_per_chunk: 1234') ==> redimension(source, template, false, cells_per_chunk: 1234)
    #' @return A new arrayOp 
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
    load_file = function(filepath, aio_settings = list(), field_conversion = NULL, file_headers = NULL){
      
      if(!.has_len(file_headers))
        file_headers = self$dims_n_attrs

      lookup = structure(0:(length(file_headers) - 1), names = file_headers)
      colIndexes = vapply(self$dims_n_attrs, function(x) lookup[x], integer(1))
      colIndexes = colIndexes[!is.na(colIndexes)]

      fieldTypes = self$get_field_types(names(colIndexes), .raw = TRUE)
      
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
      
      aioExpr = afl(afl_join_fields(settingItems) | aio_input)
      applyExpr = afl(aioExpr | apply(afl_join_fields(names(fieldTypes), castedItems)))
      projectedExpr = afl(applyExpr | project(names(fieldTypes)))
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
    match = function(template, op_mode, lower_bound = NULL, upper_bound = NULL, field_mapping = NULL){
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
      
      filter_mode = function(){
        assert(inherits(template, 'data.frame'), 
          "ERROR: ArrayOp$match: filter mode: template must be a data.frame, but got: %s", class(template))
        unmatchedCols = names(template) %-% self$dims_n_attrs %-% lower_bound %-% upper_bound
        assert_not_has_len(unmatchedCols, 
          "ERROR: ArrayOp$match: filter mode: template field(s) not matching the source: '%s'",
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
          self | cross_between(
            template | apply(applyExpr) | 
              project(regionLowAttrNames, regionHighAttrNames))
        )
        return(afl_literal)
      }
      
      index_lookup_mode = function() {
        assert(inherits(template, 'ArrayOpBase'), 
               "ERROR: ArrayOp$match: index_lookup mode: template must be an ArrayOp instance, but got: [%s]", paste(class(template), collapse = ","))
        assert(length(template$attrs) == 1 && length(template$dims) == 1,
               "ERROR: ArrayOp$match: index_lookup mode: template must have only one dimension and one attribute")
        if(is.null(field_mapping)){
          matchedFields = template$attrs %n% self$dims_n_attrs # find matched fields from template's attrs only (not in dims)
          assert(length(matchedFields) == 1, 
                 "ERROR: ArrayOp$match: index_lookup mode: param 'field_mapping' == NULL, but template field '%s' does not match any source fields",
                 template$attrs)
          field_mapping = new_named_list(matchedFields, names = matchedFields)
        }
        assert(length(field_mapping) == 1, 
               "ERROR: ArrayOp$match: index_lookup mode: there should be exactly one template attribute that matches source's fields, but %d field(s) found: %s",
               length(field_mapping), paste(field_mapping, collapse = ","))
        assert_not_has_len(lower_bound,
                       "ERROR: ArrayOp$match: index_lookup mode: param 'lower_bound' is not allowed in this mode.")
        assert_not_has_len(upper_bound,
                       "ERROR: ArrayOp$match: index_lookup mode: param 'upper_bound' is not allowed in this mode.")
        
        
        if(is.null(names(field_mapping))){ 
          # e.g. field_mapping = 'field_a' 
          sourceField = as.character(field_mapping)
          templateField = template$attrs
        } else {
          sourceField = names(field_mapping)
          templateField = as.character(field_mapping)
        }
        assert(sourceField %in% self$dims_n_attrs,
               "ERROR: ArrayOp$match: index_lookup mode: '%s' is not a valid source field.", sourceField)
        assert(templateField %in% template$attrs,
               "ERROR: ArrayOp$match: index_lookup mode: '%s' is not a valid template field.", templateField)
        
        isSourceAttrMatched = sourceField %in% self$attrs
        sourceMatchField = if(isSourceAttrMatched) sourceField else sprintf("attr_%s", sourceField)
        
        sourceOp = if(isSourceAttrMatched) self else {
          afl(self | apply(sourceMatchField, sourceField))
        }
        indexName = sprintf("index_%s", sourceField)
        templateOp = template
        if(templateField %in% self$dims_n_attrs) { 
          # if the template field name exist on the source too, there will be field name conflicts when we 'project'
          templateOp = template$reshape(new_named_list(templateField, names = sprintf("alt_%s_", templateField)))
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
        stopf("ERROR: ArrayOp$match: unknown op_mode '%s'.", op_mode)
      )()
      self$create_new_with_same_schema(aflExpr)
    }
    ,
    #' @description 
    #' Create a new ArrayOp instance from 'build'ing a data.frame
    #' 
    #' All matching fields are built as attributes of the result ArrayOp.
    #' Build operator accepts compound attribute types, so the result may have something like "build(<aa:string not null, ...)"
    #' @param df a data.frame, where all column names must all validate template fields.
    #' @param artificial_field A field name used as the artificial dimension name in `build` scidb operator
    #' By default, a random string is generated, and the dimension starts from 0. 
    #' A customized dimension can be provided e.g. `z=42:*` or `z=0:*:0:1000`.
    #' @return A new ArrayOp instance whose attributes share the same name and data types with the template's fields.
    build_new = function(df,  artificial_field = .random_attr_name()) {
#       assert(inherits(df, c('data.frame')), "ERROR: ArrayOp$build_new: unknown df class '%s'. 
# Only data.frame is supported", class(df))
      assert_inherits(df, "data.frame")
      
      builtAttrs = names(df)
      
      dfNonMatchingCols = builtAttrs %-% self$dims_n_attrs
      assert_not_has_len(dfNonMatchingCols, "ERROR: ArrayOp$build_new: df column(s) '%s' not found in template %s",
        paste(dfNonMatchingCols, collapse = ','), self$to_afl())
      
      builtDtypes = self$get_field_types(builtAttrs, .raw = T)
      
      attrStr = paste(builtAttrs, builtDtypes, collapse = ',', sep = ':')
      # convert columns to escaped strings
      colStrs = 
          lapply(builtAttrs, function(x) {
            colScidbType = builtDtypes[[x]]
            vec = df[[x]]
            switch(
              colScidbType,
              # "string" = sprintf("\\'%s\\'", gsub("(['\\])", "\\\\\\\\\\1", vec)),
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
      
      afl_literal = sprintf("build(<%s>[%s], '[%s]', true)", 
        attrStr, artificial_field, contentStr)
      
      builtDtypes[[artificial_field]] = 'int64'
      return(self$create_new(afl_literal, artificial_field, builtAttrs, 
        dtypes = builtDtypes))
    }
    
    ,
    #' @description 
    #' Create a new ArrayOp instance that added null values to the missing fields compared to a reference ArrayOp
    #' 
    #' @param reference An ArrayOp instance that has fields absent in the source; or a named list.
    #' If reference is ArrayOp, a named list is generated by calling `reference$get_field_types(reference$attrs, .raw = T)`
    #' @return A new arrayOp 
    set_null_fields = function(reference) {
      
      refDtypes = if(is.list(reference)) reference else reference$get_field_types(reference$attrs, .raw = T)
      # Fields missing in source but present in reference
      missingFields = names(refDtypes) %-% self$dims_n_attrs
      missingDtypes = refDtypes[missingFields]
      
      nullStrings = sprintf("%s(null)", missingDtypes)
      self$create_new(afl(self | apply(afl_join_fields(missingFields, nullStrings))),
                      dims = self$dims, attrs = self$attrs %u% missingFields, 
                      dtypes = utils::modifyList(self$get_field_types(), missingDtypes),
                      dim_specs = self$get_dim_specs())
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
    #' @param source_auto_increment a named number vector e.g. c(z=0), where the name is a source field and value is the starting index
    #' @param target_auto_increment a named number vector e.g. c(aid=0), where the name is a target field and value is the starting index.
    #' Here the `target_auto_increment` param only affects the initial load when the field is still null in the target array.
    #' @param anti_collision_field a target dimension name which exsits only to resolve cell collision 
    #' (ie. cells with the same dimension coordinate).
    #' @return A new arrayOp 
    set_auto_fields = function(target, source_auto_increment = NULL, target_auto_increment = NULL, anti_collision_field = NULL
                               ,join_setting = NULL, source_anti_collision_dim_spec = NULL) {
      assert_inherits(target, "ArrayOpBase")
      
      result = self
        
      # If there is an auto-incremnt field, it needs to be inferred from the params
      # After this step, 'src' is an updated ArrayOp with auto-incremented id calculated (during AFL execution only due to lazy evaluation)
      if(.has_len(target_auto_increment)){
        result = result$.private$set_auto_increment_field(target, 
          source_field = names(source_auto_increment), ref_field = names(target_auto_increment), 
          source_start = source_auto_increment, ref_start = target_auto_increment)
      }
      
      # If there is anti_collision_field, more actions are required to avoid cell collision
      # After this step, 'src' is an updated ArrayOp with auto_collision_field set properly
      if(.has_len(anti_collision_field)){
        result = result$.private$set_anti_collision_field(target, anti_collision_field, join_setting = join_setting, 
                                                 source_anti_collision_dim_spec = source_anti_collision_dim_spec)
      }
      # browser()
      result = private$conn$array_op_from_afl(result$to_afl())
      return(result)
    }
    ,
    mutate = function(...){
      fieldExprs = list(...)
      selfAttrs = new_named_list(self$attrs, self$attrs)
      finalFields = utils::modifyList(selfAttrs, fieldExprs)
      
      assert_unique_named_list(fieldExprs)
      assert_empty(names(fieldExprs) %n% self$dims,
                   "Cannot mutate dimension(s): [{.value}].")
      
      reshaped = private$reshape_attrs(finalFields)
      private$conn$array_op_from_afl(reshaped$to_afl())
    }
    ,
    transmute = function(...){
      reshaped = private$reshape_attrs(list(...))
      private$conn$array_op_from_afl(reshaped$to_afl())
    }
    ,
    # mutate an array by field expressions
    # return: transient array_op with same schema as self
    i_mutate = function(..., artificial_field = utility$random_field_name()) {
      data_source = list(...)
      
      mutatedFields = names(data_source)
      # assert(length(mutatedFields) == length(data_source), 
      #        "ERROR: ArrayOp$mutate: param 'data_source', if a list, must have names as the mutated fields, and values as mutated values/expressions.")
      
      assert_unique_named_list(data_source)
      assert_empty(mutatedFields %n% self$dims,
                   "Cannot mutate dimension(s): [{.value}].")
      
      assert_empty(mutatedFields %-% self$dims_n_attrs,
                   "New field(s) [{.value}] are not allowed in i_mutate. Please use ArrayOp$mutate(...) method to add/remove fields.")
      
      nullFileds = Filter(is.null, data_source)
      assert_empty(names(nullFileds), 
                   "Cannot remove existing field(s): [{.value}]. Please use ArrayOp$mutate(...) method to add/remove fields.",
                   )
      
      if(!.has_len(mutatedFields %n% self$dims)) { # Only attrs muated
        do.call(self$mutate, data_source)
      } else {
        self$reshape(utils::modifyList(as.list(self$dims_n_attrs), data_source), 
                     dim_mode = 'drop', artificial_field = artificial_field)$
          change_schema(self, strict = FALSE)
      }
    }
    ,
    i_mutate_by = function(data_array, 
                           keys = NULL, 
                           updated_fields = NULL,
                           .redimension_setting = NULL, 
                           .join_setting = NULL) {
      assert_inherits(data_array, "ArrayOpBase")
      
      data_source = data_array
      
      # updateFields default to the overlapping attrs between source and self
      updatedFields = if(.has_len(updated_fields)) updated_fields else data_source$attrs %n% self$attrs
      reservedFields = self$attrs %-% updatedFields
      needTransform = !(length(data_source$dims) == length(self$dims) && all(data_source$dims == self$dims))
      
      if(needTransform){
        assert_has_len(keys, "ERROR: ArrayOp$mutate: param 'keys' cannot be empty if data_source is an arrayOp")
        assert_has_len(updated_fields, "ERROR: ArrayOp$mutate: param 'updated_fields' cannot be empty if data_source is an arrayOp")
        assert_no_fields(
          (keys %-% self$dims_n_attrs) %u% (keys %-% data_source$dims_n_attrs),
          "ERROR: ArrayOp$mutate: param 'keys' has invalid field(s) [%s]")
        assert_no_fields(
          (updated_fields %-% self$dims_n_attrs) %u% (updated_fields %-% data_source$dims_n_attrs),
          "ERROR: ArrayOp$mutate: param 'updated_fields' has invalid field(s) [%s]")
        data_source = private$key_to_coordinates(data_source, keys = keys, reserved_fields = updatedFields, 
                                                 .redimension_setting = .redimension_setting, .join_setting = .join_setting)
      }
      # mutate_by_arrayop(data_source, updatedFields, reservedFields)
      assert_has_len(updatedFields, "ERROR: ArrayOp$mutate: param 'data_source' does not have any target attributes to mutate.")
      self$create_new_with_same_schema(
        afl(
          data_source$reshape(updatedFields, .force_project = FALSE) | join( 
            .ifelse(.has_len(reservedFields), self$reshape(reservedFields), self)
          )
        )
      )$reshape(self$attrs)
    }
    ,
    #' @description 
    #' Generate a mutated arrayOp of the source `self` by the `data_source`.
    #' 
    #' Neither `self` or `data_source` is mutated. The resultant arrayOp is a 'mutated' version of `self` in the sense 
    #' that `result` has the exact same schema as `self` and a subset of cells that match to `self`. 
    #' 
    #' This function works in two modes: 1. `data_source` is a named list that specifies the expression for mutation.
    #' 2. `data_source` is an arrayOp that contains the mutated data and 'key' data for cell identification.
    #'
    #' Mode-1: `data_source` is a named list.
    #' The `data_source` list should only contain names as the mutated fields and string values as the mutate expression.
    #' All other fields not present in the list would remain the same. 
    #' Normally, the list should only have `self`'s attributes as the mutated fields. 
    #' But if `data_source` contains target's dimensions, then the number of matching cells should be no more than 1, 
    #' otherwise mutiple matching cells would cause dimension collision error thrown by scidb. 
    #' 
    #' Mode-2: `data_source` is an arrayOp instance.
    #' The `data_source` arrayOp must have two types of data. One type is the 'key' data that are used to identify which
    #' cells to mutate. The other is the 'mutated' data of the matching cells. 
    #' If the `keys` have all `self`'s dimensions, then no join is needed to match cell coordinates.
    #' Otherwise we need to map `keys` to cell coordinates using join and redimension. 
    #' 
    #' @param data_source A named list of mutated field expressions or an arrayOp instance.
    #' @param keys Which fields of `data_source` to identify the matching cells. Only applicable when `data_source` is arrayOp.
    #' @param updated_fields Which fields of `data_source` to mutate for the matching cells. 
    #' Only applicable when `data_source` is arrayOp.
    #' @param artificial_field The attribute name in unpack if we need to drop `self`'s dimensions. 
    #' Only applicable when `data_source` is a list and contains `self`'s dimensions, which is rare.
    #' @param .redimension_setting Only applicable when `data_source` is arrayOp.
    #' @param .join_setting Only applicable when `data_source` is arrayOp and it does not have all `self`'s dimensions.
    #' @return a new arrayOp instance that carries the mutated data and has the exact same schema as the target
    mutate_old = function(data_source, keys = NULL, updated_fields = NULL, artificial_field = .random_attr_name(), 
                      .redimension_setting = NULL, .join_setting = NULL) {
      assert(inherits(data_source, c('list', 'ArrayOpBase')), 
             "ERROR: ArrayOpBase$mutate: param 'data_source' must be a named list or ArrayOp instance, but got: [%s]",
             paste(class(data_source), collapse = ', '))
      
      mutate_by_field_exprs = function(){
        mutatedFields = names(data_source)
        assert(length(mutatedFields) == length(data_source), 
               "ERROR: ArrayOp$mutate: param 'data_source', if a list, must have names as the mutated fields, and values as mutated values/expressions.")
        absentFields = mutatedFields %-% self$dims_n_attrs
        assert_not_has_len(absentFields, "ERROR: ArrayOp$mutate: %d unrecognized mutated field(s): %s", 
                           length(absentFields), paste(absentFields, collapse = ', '))
        if(!.has_len(mutatedFields %n% self$dims)) { # Only attrs muated
          self$reshape(utils::modifyList(as.list(self$attrs), data_source))
        } else {
          self$reshape(utils::modifyList(as.list(self$dims_n_attrs), data_source), 
                       dim_mode = 'drop', artificial_field = artificial_field)$
              change_schema(self, strict = FALSE)
        }
      }
      
      ## source should have the same dimensions as the target(self)
      mutate_by_arrayop = function(source, updatedFields, reservedFields) {
        # updatedFields = data_source$attrs %n% self$attrs
        # reservedFields = self$attrs %-% updatedFields
        
        assert_has_len(updatedFields, "ERROR: ArrayOp$mutate: param 'data_source' does not have any target attributes to mutate.")
        self$create_new_with_same_schema(
          afl(
            source$reshape(updatedFields, .force_project = FALSE) | join( 
              .ifelse(.has_len(reservedFields), self$reshape(reservedFields), self)
            )
          )
        )$reshape(self$attrs)
      }
      
      if(is.list(data_source)){
        mutate_by_field_exprs()
      } else {
        ## data_source is an arrayOp instance
        # assume data_soruce has the same dimension with the target
        
        # updateFields default to the overlapping attrs between source and self
        updatedFields = if(.has_len(updated_fields)) updated_fields else data_source$attrs %n% self$attrs
        reservedFields = self$attrs %-% updatedFields
        needTransform = !(length(data_source$dims) == length(self$dims) && all(data_source$dims == self$dims))
        
        if(needTransform){
          assert_has_len(keys, "ERROR: ArrayOp$mutate: param 'keys' cannot be empty if data_source is an arrayOp")
          assert_has_len(updated_fields, "ERROR: ArrayOp$mutate: param 'updated_fields' cannot be empty if data_source is an arrayOp")
          assert_no_fields(
            (keys %-% self$dims_n_attrs) %u% (keys %-% data_source$dims_n_attrs),
            "ERROR: ArrayOp$mutate: param 'keys' has invalid field(s) [%s]")
          assert_no_fields(
            (updated_fields %-% self$dims_n_attrs) %u% (updated_fields %-% data_source$dims_n_attrs),
            "ERROR: ArrayOp$mutate: param 'updated_fields' has invalid field(s) [%s]")
          data_source = private$key_to_coordinates(data_source, keys = keys, reserved_fields = updatedFields, 
                                                   .redimension_setting = .redimension_setting, .join_setting = .join_setting)
        }
        mutate_by_arrayop(data_source, updatedFields, reservedFields)
      }
    }
    ,
    #' @description 
    #' Update Target array with self's content
    #' 
    #' Similar behavior to scidb insert operator
    #' Fields of self and Target must match by raw data type. Field names are irrelevant.
    #' 
    #' @param target An arrayOp instance where self's content is written to.
    #' @return A new arrayOp that encapsulates the insert operation
    update = function(target) {
      assert(inherits(target, 'ArrayOpBase'), "ERROR: ArrayOp$update: param 'target' must be an arrayOp, but got [%s]",
             paste(class(target), collapse = ', '))
      private$afl_insert(target)
    }
    ,
    #' @description 
    #' Overwrite Target array with self's content
    #' 
    #' Warning: Target's content will be erased and filled with self's content.
    #' Similar behavior to scidb `store` operator. 
    #' Fields of self and Target must match by raw data type. Field names are irrelevant.
    #' See `ArrayOp$change_schema`
    #' 
    #' @param target An arrayOp instance where self's content is written to.
    #' @return A new arrayOp that encapsulates the insert operation
    overwrite = function(target) {
      assert(inherits(target, 'ArrayOpBase'), "ERROR: ArrayOp$target: param 'target' must be an arrayOp, but got [%s]",
             paste(class(target), collapse = ', '))
      private$afl_store(target)
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
    #' @param renamed Rename fields
    #' @param added New fields
    #' @param excluded Fields to exclude from `self`
    #' @param dtypes Data types for any existing or new fields, which default to `self`'s data types if available.
    #' @param dim_specs A list of array dimension specs if dimensions are changed.
    #' @return A new arrayOp instance
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
        attrs = as.character(replace(attrs, attrs %in% renamedOldFields, .remove_null_values(renamed[attrs])))
        dims = as.character(replace(dims, dims %in% renamedOldFields, .remove_null_values(renamed[dims])))
        namesOldDtypes = names(oldDtypes)
        names(oldDtypes) <- replace(namesOldDtypes, namesOldDtypes %in% renamedOldFields, .remove_null_values(renamed[namesOldDtypes]))
        namesOldDimSpecs = names(oldDimSpecs)
        names(oldDimSpecs) <- replace(namesOldDimSpecs, namesOldDimSpecs %in% renamedOldFields, .remove_null_values(renamed[namesOldDimSpecs]))
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

    # Common array operators ------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Get the first `n` rows of an array
    #' @param n How many rows to take
    #' @param skip How many rows to skip before taking
    limit = function(n = 5, skip = NULL) {
      assert_single_number(n)
      assert(is.null(skip) || is.numeric(skip))
      aflStr = if(is.null(skip)) afl(self | limit(n)) else afl(self | limit(n, skip))
      self$create_new_with_same_schema(aflStr)
    }
    ,
    row_count = function(){
      private$conn$query_attrs(afl(self | op_count))[["count"]]
    }
    ,
    summarize = function(){
      private$conn$query_attrs(afl(self | summarize))
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
    #' Return a schema representation of the ArrayOp <attr1 [, attr2 ...]> \[dim1 [;dim2]\]
    #' 
    #' @return An AFL string
    to_schema_str = function() {
      attrStr = paste(self$attrs, self$get_field_types(self$attrs), sep = ':', collapse = ',')
      dimStrItems = mapply(function(dimName, dimSpec){
        if(.has_len(dimSpec) && nchar(dimSpec) > 0) sprintf("%s=%s", dimName, dimSpec)
        else dimName
      }, self$dims, self$get_dim_specs())
      dimStr = paste(dimStrItems, collapse = ';')
      sprintf("<%s> [%s]", attrStr, dimStr)
    }
    # db functions ----
    ,
    to_df = function() {
      private$conn$query(private$to_df_afl())
    }
    ,
    to_df_attrs = function() {
      private$conn$query_attrs(private$to_df_afl(drop_dims = TRUE))
    }
    ,
    execute = function() {
      private$conn$execute(self$to_afl())
      invisible(NULL)
    }
    ,
    versions = function(){
      private$conn$query_attrs(afl(self | versions))
    }
    ,
    remove_versions = function(version_id = NULL){
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
    remove_self = function(){
      private$conn$execute(afl(self | remove))
      invisible(NULL)
    }
    ,
    inner_join = function(right, ...) {
      private$join(self, right,
        ..., .auto_select = T)  
    }
    ,
    left_join = function(right, ...) {
      private$join(self, right,
        settings = list(left_outer=1),
        ..., .auto_select = T)  
    }
    ,
    right_join = function(right, ...) {
      private$join(self, right,
        settings = list(right_outer=1),
        ..., .auto_select = T)  
    }
    ,
    semi_join = function(df, 
                         field_mapping = NULL,
                         lower_bound = NULL,
                         upper_bound = NULL,
                         mode = "auto",
                         ...
                         ){
        repo = private$conn$private$repo
        repo$semi_join(df, self, 
                       field_mapping = field_mapping,
                       lower_bound = lower_bound,
                       upper_bound = upper_bound,
                       mode = mode,
                       ...
                       )
    }
    ,
    persist = function(
      save_array_name = NULL,
      .temp = FALSE,
      .gc = TRUE
    ){
      # No need to store an already persistent arrary
      if(self$is_persistent()) return(self) 
      private$conn$array_op_from_stored_afl(private$to_df_afl(), 
                                    save_array_name = save_array_name,
                                    .temp = .temp,
                                    .gc = .gc
                                    )
    }
    ,
    is_persistent = function(){
      !grepl("\\(", self$to_afl()) && 
        grepl("^((\\w+)\\.)?(\\w+)$", self$to_afl())
    }  
    ,
    exists_persistent_array = function() {
      self$is_persistent() && nrow(self$array_meta_data()) == 1L
    }
    ,
    array_meta_data = function(){
      assertf(self$is_persistent(), 
              "{.symbol} is not a persistent scidb array. Array meta data is only available for persistent scidb arrays.")
      full_array_name = self$to_afl()
      ns = gsub("^((\\w+)\\.)?(\\w+)$", "\\2", full_array_name)
      bare_name = gsub("^((\\w+)\\.)?(\\w+)$", "\\3", full_array_name)
      query_str = if(ns == "") "list('arrays')" else 
        sprintf("list('arrays', ns:%s)", ns)
      private$conn$query_attrs(sprintf("filter(%s, name = '%s')", query_str, bare_name))
    }
    
    # Old -------------------------------------------------------------------------------------------------------------
    ,
    #' @description 
    #' Set ArrayOp meta data directly
    #'
    #' Useful in keeping reference of other relevant R objects that should have a same life cycle with the ArrayOp instance.
    #' @param key A string key
    #' @param value A value of any type
    #' @return NULL
    .set_meta = function(key, value) private$set_meta(key, value)
    ,
    #' @description 
    #' Get ArrayOp meta data directly
    #' @param key A string key
    #' @return An metadata object of any type registered with `key`
    .get_meta = function(key) private$get_meta(key)
    ,
    finalize = function() {
      refKey = '.ref'
      refObj = self$.get_meta(refKey)
      if(!is.null(refObj)) {
        
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
        }
        
        # printf("finialize arrayop: %s", self$to_afl())
        if(is.list(refObj)) sapply(refObj, cleanUpObj)
        else cleanUpObj(refObj)
        
        self$.set_meta(refKey, NULL)
      }
    }
  )
)

