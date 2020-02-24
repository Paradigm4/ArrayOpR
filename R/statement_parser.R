.DEFAULT_ARRAYOP_VERBS = strsplit(
  "create_new create_new_with_same_schema spawn
  where select reshape
  join match
  build_new load_file set_auto_increment_field
  write_to
  to_afl to_df_afl to_schema_str
  get_field_types get_dim_specs get_absent_fields
  create_array_cmd remove_array_cmd remove_array_versions_cmd",
  '\\s+')[[1]]

StatementParser <- R6::R6Class(
  "StatementParser", portable = FALSE,
  ## Public functions ------
  public = list(
    initialize = function(arrayop_verbs = .DEFAULT_ARRAYOP_VERBS) {
      private$.arrayop_verbs = arrayop_verbs
      private$.arrayop_verb_magic_names = magic_names = sapply(arrayop_verbs, function(x) paste0('%', x, '%'))
      private$.arrayop_verb_lookup = structure(sapply(arrayop_verbs, as.name), names = magic_names)
      private$.verb_regex = sprintf("\\b(%s)\\b", paste(arrayop_verbs, collapse='|'))
    }
    ,
    parse_arrayop_expr = function(stmt) {
      walkthrough = function(x) {
        if(is.call(x)){
          funcName = as.character(x[[1]])
          if(funcName %in% .arrayop_verb_magic_names){
            # We can safely assume that length(x) >= 2, i.e. at least one operand
            # 'operands' in the raw function, e.g. a join b => operands = c(a, b); a op_count () => operand = c(a)
            operands = lapply(x[2: length(x)], walkthrough)
            instanceFunc = as.call(c(quote(`$`), operands[[1]], .arrayop_verb_lookup[[funcName]]))
            instanceParams = if(length(operands) > 1) operands[2: length(operands)] else NULL
            newCall = as.call(c(instanceFunc, instanceParams))
            newCall
          }
          else{
            as.call(lapply(x, walkthrough))
          }
        } else x
      }
      walkthrough(parse_raw_expr(stmt))
    }
    ,
    parse_raw_expr = function(stmt) {
      exprStmt = replace_verbs(replace_parenthese(stmt))
      modify_expression(
        rlang::parse_expr(exprStmt)
      )
    }
  )
  ,
  ## Private fields/functions ----
  private = list(
    .arrayop_verbs = NULL
    , .arrayop_verb_magic_names = NULL
    , .arrayop_verb_lookup = NULL
    , .verb_regex = NULL
    ,
    replace_verbs = function(stmt) {
      gsub(.verb_regex, "%\\1%", stmt)
    }
    ,
    replace_parenthese = function(stmt) {
      gsub("(\\s+)\\(|^\\(", "\\1list(", stmt)
    }
    ,
    modify_expression = function(ex) {
      walkthrough = function(x) {
        if(is.call(x)){
          operand2 = if(length(x) >= 3) x[[3]] else NULL
          # quote(list) is the placeholder for extra function params
          if(is.call(operand2) && identical(operand2[[1]], quote(list))){
            leftover = operand2[2:length(operand2)]
            newList = if(length(operand2) < 2) x[1:2] else c(x[[1]], x[[2]], as.list(leftover))
            as.call(lapply(newList, walkthrough))
          } else if(identical(x[[1]], quote(list))) {
            if(length(x) == 2) walkthrough(x[[2]])
            else stop(sprintf("ERROR: multiple items in %s should only be used as function parameters.", deparse(x)))
          } else {
            as.call(lapply(x, walkthrough))
          }
        } else {
          x
        }
      }
      walkthrough(ex)
    }
  )
)
