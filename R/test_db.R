#
# This script is for specific scidb version tests
#

# Debugging -------------------------------------------------------------------------------------------------------

debugging_db = function() {
  result = scidb::scidbconnect(host = "localhost",
                         username = 'scidbadmin',
                         password = readLines('~/.bms_password'),
                         port = 8083,
                         protocol = "https")
  options(scidb.aio = TRUE)
  options(scidb.result_size_limit = 1*1024)
  result
}

setup_namespace = function(db) {
  scidb::iquery(db, sprintf("create_namespace('%s')", NS), return = F)
}

debug = function(db) {
  repo = newRepo(default_namespace = 'test_arrayop', db = db)
}

NS = 'test_arrayop'
VariantArray = sprintf('%s.Variant', NS)

if(! exists('db'))
  db = debugging_db()
if(! exists('repo'))
  # setup repo
  repo = newRepo(default_namespace = NS, db = db)

# Run at user side ------------------------------------------------------------------------------------------------


#' Run tests with a scidb connection
#' @param db a scidb connection returned by calling `scidb::scidbconnect`
#' @param namespace Namespace to run tests in
#' @export
run_tests_with_scidb_connection = function(db, namespace) {
  # create_variants_array = function() {
  #   try(repo$execute(afl(VariantArray %remove% NULL)))
  #   repo$execute(
  #     sprintf("create array %s variant_id:int64,
  #                           ref:string,
  #                           alt:string,
  #                           extra:string> [chrom=1:24:0:1;
  #                           pos=1:*:0:1000000,
  #                           alt_id=0:*:0:100]", VariantArray)
  #   )
  #   repo$register_schema_alias_by_array_name('V', VariantArray, is_full_name = T)
  # }
  
  load_vcf_files = function() {
    # V = repo$get_alias_schema('V')
    
  }
  
}

log_job = function(job, msg) {
  print(sprintf("Start %s...", msg))
  force(job)
  print(sprintf("Finish %s", msg))
}

simple_load = function(db, VariantArray) {
  try(repo$execute(afl(VariantArray %remove% NULL)))
  repo$execute(
    sprintf("create array %s <
                            ref:string,
                            alt:string,
                            extra:string> [chrom=1:24:0:1;
                            pos=1:*:0:1000000]", VariantArray)
  )
  repo$register_schema_alias_by_array_name('V', VariantArray, is_full_name = T)
  V = repo$get_alias_schema('V')
  initVcf = system.file('extdata', 'init.vcf', package = 'arrayopTemp', mustWork = T)
  loaded = V$load_file(initVcf, aio_settings = list(header = 1), file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
  repo$execute(loaded$write_to(V))
}
