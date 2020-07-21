## Internal ----
config = yaml::yaml.load_file("repo.yaml")
# Run tests directly from console
# config = yaml::yaml.load_file("tests/testthat/db/repo.yaml")
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





