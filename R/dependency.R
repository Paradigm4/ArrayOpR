# ArrayOp package dependency, implemented in a class

ArrayOpDep <- setRefClass("ArrayOpDep",
  fields = c('df_to_arrayop_func', 'db')
)

#
# Create a global instance for dependency configuration in the ArrayOp package scope
#

.df_to_arrayop_func = function(df) {
  uploaded = scidb::as.scidb(DEP$db, df)
  # Hold a reference to 'uploaded' scidb array so that gc() won't garbage collect it.
  op = ArraySchema$new(uploaded@name, namespace = NULL, dims = 'i', attrs = names(df), info = list(refs=uploaded))
  return(op)
}

# Client code needs to inject 'db' to DEP instance
#' @export
DEP = ArrayOpDep(df_to_arrayop_func = .df_to_arrayop_func, db = NULL)
