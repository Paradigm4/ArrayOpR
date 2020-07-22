## Internal ----
config = yaml::yaml.load_file("repo.yaml")
NS = config$namespace

.remove_arrays_from_namespace = function(ns = NS){
  arrayNames = repo$query(
    sprintf("project(list(ns:%s), name)", ns)
  )[['name']]
  for(arr in arrayNames){
    fullName = sprintf("%s.%s", ns, arr)
    repo$execute(afl(fullName | remove))
  }
}

.try_create_ns = function(ns = NS) {
  tryCatch(
    {
      repo$execute(afl(ns | create_namespace))
      catf("Created namespace '%s'\n", ns)
    },
    error = function(e){
      catf("Namespace '%s' already exists or cannot be created.\n", ns)
    }
  )
}

.try_drop_ns = function(ns = NS) {
  tryCatch(
    {
      repo$execute(afl(ns | drop_namespace))
      catf("Dropped namespace '%s' \n", ns)
    },
    error = function(e) {
      catf("Failed to drop namespace '%s' \n", ns)
    }
  )
}

## Exported functions ----

db_setup = function() {
  .try_create_ns()
  .remove_arrays_from_namespace()
}

db_cleanup = function() {
  .remove_arrays_from_namespace()
  .try_drop_ns()
}

cleanup_after_each_test = function() {
  .remove_arrays_from_namespace()
}

## Shared code for individual tests in this folder ----
`%>%` = dplyr::`%>%`

# arrays loaded from the config file
CfgArrays = repo$load_arrayops_from_config(config)
# Get array's raw names (without namespace)
CfgArraysRawNames = sapply(CfgArrays, function(x) gsub("^[^\\.]+\\.", '', x$to_afl()))

# Get array names without namespace
get_array_names = function() {
  repo$query(
    sprintf("project(list(ns:%s), name)", NS), only_attributes = T
  )[['name']]
}

# Create a local arrayOp instance in R
create_local_arrayop = function(name, schema) {
  repo$get_array(sprintf(
    "%s.%s %s", NS, name, schema
  ))
}

reset_array_with_content = function(target, content, recreate = TRUE) {
  # Reset array
  if(recreate){
    try(repo$.remove_array(target), silent = T)
    repo$.create_array(target)
  }
  # Upload data 
  repo$execute(
    target$
      build_new(content)$
      change_schema(target)$
      overwrite(target)
  )
}

