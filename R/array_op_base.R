# A base class for other specialized array operands/operations, e.g. SubsetOp, JoinOp, ArraySchema

# Field type constants
.DIM = 'dimension'
.ATR = 'attribute'
.OWN = 'owned: dimension + attribute'
.AD = 'attribute + dimension'
.SEL = 'selected'
.DT = 'field_data_types'

#' Base class of all ArrayOp classes
#' 
#' @export ArrayOpBase
#' @exportClass ArrayOpBase
ArrayOpBase <- R6::R6Class("ArrayOpBase",
  private = list(
    info = NULL
  ),
  active = list(
    dims = function() as.character(c()),
    attrs = function() as.character(c()),
    selected = function() as.character(c()),
    dims_n_attrs = function() c(self$dims, self$attrs),
    attrs_n_dims = function() c(self$attrs, self$dims)
  ),


  # Public ----------------------------------------------------------------------------------------------------------

  public = list(
    
    initialize = function(info = NULL) {
      private$info = info  
    }
    ,
    #' @description 
    #' Return ArrayOp field types
    #'
    #' NOTE: private$info has to be defined, otherwise returns NULL
    #' @param field_names R character
    #' @return a named list as `field_names`, where absent fields or fields without data types are dropped silently.
    get_field_types = function(field_names){
      dtypes = private$info[[.DT]][field_names]
      return(plyr::compact(dtypes))
    }
    ,
    #' @description 
    #' Validate fields existence according the 'type' which defaults to 'owned' fields
    #' 
    #' This function is useful for validating fields in use cases:
    #    1. whether fields are valid for 'select' or 'filter';
    #    2. whether fields can be used as keys in JoinOp
    get_absent_fields = function(fieldNames) {
      absentMarks = !(fieldNames %in% self$dims_n_attrs)
      return(fieldNames[absentMarks])
    }
    ,
    #' @description 
    #' Validate a filter expression ('filterExpr' must be a single R call expression)
    #'
    #' Current only report errors on:
    #'   1. Name symbols that are not existing schema fields
    #'   2. Non-atomic 'values'
    #' @param filterExpr An rlang::expr
    #' @return A list object with named elements:
    #   success:bool, absent_fields: c(''), error_msgs: c('')
    validate_filter_expr = function(filterExpr) {
      allFieldNames <- self$dims_n_attrs

      absentFields = c()
      errorMsgs = c()

      # a recurrsive function that traverses every element in filterExpr
      traverseSingleExpr <- function(rExpr) {
        if (is.name(rExpr)) {
          symbolName <- as.character(rExpr)
          if (!is.element(symbolName, allFieldNames)) {
            assign('absentFields', c(absentFields, symbolName), inherits = TRUE)
          }
        } else if (is.call(rExpr)) {
          # rExpr is a call, then traverse its args
          lapply(rExpr[-1], traverseSingleExpr)
        } else {
          # Neither a name symbol nor a call node, then must be an atomic vector, e.g. 42, c(3, 4), 'abc'
          if (!is.atomic(rExpr)) {
            assign('errorMsgs',
              c(errorMsgs, sprintf("Non-atomic '%s' object can not be used in filter expression", class(rExpr))),
              inherits = TRUE)
          }
        }
      }

      traverseSingleExpr(filterExpr)

      return(list(
        success = !.has_len(absentFields) && !.has_len(errorMsgs),
        absent_fields = absentFields, error_msgs = errorMsgs
      ))
    },

    #' @description 
    #' Functions below translate array operations into SciDb AFL expressions/statements.
    #'
    #' NOTE: this method do not take into acount field selection
    #' Sub-classes should override this function.
    #' Doing so equips sub-classes with sensible behavior defined below.
    #' @return  AFL when self used as an operand in another parent operation without considering fields selection
    .raw_afl = function() {
      stopf('raw_afl function NOT implemented in %s class', class(self))
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
        return(self$.raw_afl())

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
        return(afl(self$.raw_afl() %project% afl_join_fields(selectedAttrs)))

      # If no attributes selected, we have to create an artificial attribute, then project it.
      # Because 'apply' doesn't work on array dimensions
      return(afl(self$.raw_afl() %apply% afl_join_fields(artificial_attr, 'null')
            %project% artificial_attr))
    },

    #' @description 
    #' Return AFL when self used as an operand in another parent operation.
    #' 
    #' Implemented by calling to_afl_explicit with `selected_fields = self$selected`
    #'
    #' @param drop_dims Whether self dimensions will be dropped in parent operations
    #' By default, dimensions are not dropped in parent operation
    #' But in some operations, dimensions are dropped or converted to attributes
    #' e.g. equi_join creates two artificial dimensions and discard any existing dimensions of two operands.
    to_afl = function(drop_dims = FALSE, artificial_attr = .random_attr_name()) {
      return(self$.to_afl_explicit(drop_dims, self$selected, artificial_attr = artificial_attr))
    },

    #' @description 
    #' Return AFL suitable for retrieving data.frame.
    #'
    #' scidb::iquery has a param `only_attributes`, which, if set TRUE, will effectively drop all dims.
    #' @param drop_dims Whether self's dimensions are dropped when generating AFL for data.frame conversion
    #' @return AFL string
    to_df_afl = function(drop_dims = FALSE, artificial_attr = .random_attr_name()) {
      return(self$to_afl(drop_dims, artificial_attr = artificial_attr))
    },

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
        res = afl(arrName %apply% afl_join_fields(artificial_attr, 'null') %project% artificial_attr)
      }
      # case-2: Regular mode. Just 'apply'/'project' for dimensions/attributes when needed.
      else{
        applyExpr = if(.has_len(applyList))
          afl(arrName %apply% paste(applyList, applyList, sep = ',', collapse = ','))
        else arrName
        res =  if(.has_len(projectList))
          afl(applyExpr %project% afl_join_fields(projectList))
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
      
      assert(is.list(fields) && .has_len(fields), "ArrayOpBase$transform 'fields' arg must be a non-empty list")
      assert(!.has_len(dtypes) || all(names(dtypes) != ''),
        "ArrayOpBase$transform 'dytypes' arg, if provided, must be a named list, i.e. list(newFieldName='newFieldType',...)")
      
      rawAfl = afl(self$to_afl() %unpack% unpack_dim_name)
      
      newFields = fields[names(fields) != '']
      if(.has_len(newFields)){
        rawAfl = afl(rawAfl %apply% afl_join_fields(names(newFields), newFields))
      }
      
      projectedFieldNames = .ifelse(is.null(names(fields)), as.character(fields),
        mapply(function(name, value){
          if(name == '') value else name
        }, names(fields), fields, USE.NAMES = F)
      )
      rawAfl = afl(rawAfl %project% afl_join_fields(projectedFieldNames))
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

