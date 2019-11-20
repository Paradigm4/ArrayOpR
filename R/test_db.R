#
# This script is for specific scidb version tests
#

`%>%` = dplyr::`%>%`

ScidbTest = R6::R6Class(
  "ScidbTest",
  private = list(
    log_job = function(job, msg, done_msg = 'Done') {
      cat(sprintf("%s ... ", msg))
      result = force(job)
      cat(sprintf("%s\n", done_msg))
      invisible(result)
    }
    ,
    file_list = NULL
    ,
    file = function(name) {
      if(!.has_len(private$file_list)){
        private$file_list = list()
      }
      full_path = private$file_list[[name]]
      if(!.has_len(full_path)){
        full_path = system.file('extdata', name, package = 'arrayop', mustWork = T)
        private$file_list[[name]] = full_path
      }
      full_path
    }
  )
  ,
  public = list(
    repo = NULL
    ,
    ns = NULL
    ,
    initialize = function(repo, ns = 'test_arrayop') {
      self$repo = repo
      self$ns = ns
    }
    ,
    #' @description 
    #' Tests entry point
    run_tests = function(create_namespace = FALSE) {
      if(create_namespace)
        self$create_namespace()
      private$log_job(self$test_simple_load(), "Test simple loading file into array")
      private$log_job(self$test_load_with_auto_increment_id(), "Test loading file with auto-increment id")
      private$log_job(self$test_where(), "Test ArrayOp$where(...)")
      private$log_job(self$test_match(), "Test ArrayOp$match(...)")
      private$log_job(self$test_load_with_dim_collision(), "Test loading file with dimension collision")
    }
    ,
    create_namespace = function() {
      try(self$repo$execute(afl(self$ns %create_namespace% NULL)))
    }
    ,
    test_simple_load = function() {
      alias = 'V1'
      VariantArray = sprintf('%s.%s', self$ns, alias)
      try(self$repo$execute(afl(VariantArray %remove% NULL)))
      self$repo$execute(
        sprintf("create array %s <
                            ref:string,
                            alt:string,
                            extra:string> [chrom=1:24:0:1;
                            pos=1:*:0:1000000]", VariantArray)
      )
      self$repo$register_schema_alias_by_array_name(alias, VariantArray, is_full_name = T)
      V = self$repo$get_alias_schema(alias)
      
      # Load all array content from a file
      loaded = V$load_file(private$file('init.vcf'), 
                           aio_settings = list(header = 1), 
                           file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
      self$repo$execute(loaded$write_to(V))
      
      resultRepo = self$repo$query(V)
      resultDf = data.table::fread(private$file('init.vcf'))
      try(assert(nrow(resultRepo) == nrow(resultDf)))
    }
    ,
    test_load_with_auto_increment_id = function() {
      alias = 'V2'
      VariantArray = sprintf('%s.%s', self$ns, alias)
      try(self$repo$execute(afl(VariantArray %remove% NULL)))
      self$repo$execute(
        sprintf("create array %s <
                            vid: int64,
                            ref:string,
                            alt:string,
                            extra:string> [chrom=1:24:0:1;
                            pos=1:*:0:1000000]", VariantArray)
      )
      self$repo$register_schema_alias_by_array_name(alias, VariantArray, is_full_name = T)
      V = self$repo$get_alias_schema(alias)
      
      minVid = 10
      batch1 = data.table::fread(private$file('init.vcf'))
      batch2 = data.table::fread(private$file('batch2.vcf'))
      
      # Load all array content from a file
      loaded = V$load_file(private$file('init.vcf'), 
                           aio_settings = list(header = 1), 
                           file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
      self$repo$execute(
        loaded$
          reshape(select = loaded$dims_n_attrs, dim_mode = 'drop', artificial_field = 'z')$
          write_to(V, source_auto_increment = c(z=0), target_auto_increment = c(vid = minVid))
      )
      
      resultRepo = self$repo$query(V)
      try({
        assert(nrow(resultRepo) == nrow(batch1))
        assert(min(resultRepo$vid) == minVid)     
        assert(max(resultRepo$vid) == minVid + nrow(batch1) - 1)     
       })
      
      # Load a second batch of variants from file
      # batch2.vcf doesn't have any cell clision with init.vcf
      loaded = V$load_file(private$file('batch2.vcf'), 
                           aio_settings = list(header = 1), 
                           file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
      self$repo$execute(
        loaded$
          reshape(select = loaded$dims_n_attrs, dim_mode = 'drop', artificial_field = 'z')$
          write_to(V, source_auto_increment = c(z=0), target_auto_increment = c(vid = minVid))
      )
      
      resultRepo = self$repo$query(V)
      try({
        assert(nrow(resultRepo) == nrow(batch1) + nrow(batch2))
        assert(min(resultRepo$vid) == minVid)     
        assert(max(resultRepo$vid) == minVid + nrow(batch1) + nrow(batch2) - 1)     
       })
      
    }
    ,
    test_where = function() {
      V = self$repo$get_alias_schema('V1')
      try({
        filtered = self$repo$query(V$where(chrom == 1))
        assert(nrow(filtered) == 2)
        assert(all(filtered$chrom == 1))
        assert(all(filtered$extra == c('variant1', 'variant2')))
        
        filtered = self$repo$query(V$where(chrom %in% !!c(1, 2)))
        assert(nrow(filtered) == 4)
        filtered = self$repo$query(V$where(chrom %in% !!c(1, 3)))
        assert(nrow(filtered) == 3)
        filtered = self$repo$query(V$where(chrom <= 3))
        assert(nrow(filtered) == 5)
        
        filtered = self$repo$query(V$where(chrom != 1 & extra %like% '.*3.*'))
        assert(nrow(filtered) == 2)
        assert(all(filtered$chrom == c(2, 3)))
        assert(all(filtered$extra == c('variant3', 'variant-chr3-1')))
      })
    }
    ,
    test_match = function() {
      V = self$repo$get_alias_schema('V1')
      df = data.frame(chrom=c(1,2))
      built = V$build_new(df)
      # Match chrom-1 and chrom-2
      try({
        for(result in list(
          self$repo$query(V$match(df, op_mode = 'filter'))
          , self$repo$query(V$match(built, op_mode = 'cross_between'))
          , self$repo$query(V$match(df, op_mode = 'filter'))
          , self$repo$query(V$match(data.frame(chrom_low=1, chrom_hi=2), op_mode = 'filter',
                                    lower_bound = list(chrom='chrom_low'), upper_bound = list(chrom='chrom_hi')))
          , self$repo$query(V$match(built, op_mode = 'cross_between', 
                                    lower_bound = list(chrom = 'chrom-3'),
                                    upper_bound = list(chrom = 'chrom+0')
                                    ))
        )){
          assert(nrow(result) == 4)
          assert(all(result$chrom == 1 || result$chrom == 2))
          assert(base::setequal(result$extra, c('variant1', 'variant2', 'variant3', 'variant4')))
        }
      })
      # More Cross-between mode tests
      try({
        assert(nrow(self$repo$query(V$match(built, op_mode = 'cross_between'))) == 4)
        assert(nrow(self$repo$query(V$match(built, op_mode = 'cross_between', 
            lower_bound = list(chrom = 'chrom-3'), upper_bound = list(chrom = 'chrom') ))) == 4)
        
        assert(nrow(self$repo$query(V$match(built, op_mode = 'cross_between',
            lower_bound = list(chrom = 'chrom-3'), upper_bound = list(chrom = 'chrom+1') ))) == 5)
      })
    }
    ,
    test_load_with_dim_collision = function() {
      alias = 'V3'
      VariantArray = sprintf('%s.%s', self$ns, alias)
      try(self$repo$execute(afl(VariantArray %remove% NULL)))
      self$repo$execute(
        sprintf("create array %s <
                            vid: int64,
                            ref:string,
                            alt:string,
                            extra:string> 
                [chrom=1:24:0:1; pos=1:*:0:1000000; alt_id=0:*:0:1000]", VariantArray)
      )
      self$repo$register_schema_alias_by_array_name(alias, VariantArray, is_full_name = T)
      V = self$repo$get_alias_schema(alias)
      
      minVid = 1
      batch1 = data.table::fread(private$file('dim_collision_1.vcf'))
      batch2 = data.table::fread(private$file('dim_collision_2.vcf'))
      
      # Load all array content from a file
      loaded = V$load_file(private$file('dim_collision_1.vcf'), 
                           aio_settings = list(header = 1), 
                           file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
      self$repo$execute(
        loaded$
          reshape(select = loaded$dims_n_attrs, dim_mode = 'drop', artificial_field = 'z')$
          write_to(V, source_auto_increment = c(z=0), target_auto_increment = c(vid = minVid), 
                   anti_collision_field = 'alt_id')
      )
      
      resultRepo = self$repo$query(V)
      try({
        assert(nrow(resultRepo) == nrow(batch1))
        assert(min(resultRepo$vid) == minVid)     
        assert(max(resultRepo$vid) == minVid + nrow(batch1) - 1)     
      })
      
      # Load a second batch of variants from file where cell collision occurs with multiple cells
      loaded = V$load_file(private$file('dim_collision_2.vcf'), 
                           aio_settings = list(header = 1), 
                           file_headers = c('chrom', 'pos', 'ref', 'alt', 'extra'))
      self$repo$execute(
        loaded$
          reshape(select = loaded$dims_n_attrs, dim_mode = 'drop', artificial_field = 'z')$
          write_to(V, source_auto_increment = c(z=0), target_auto_increment = c(vid = minVid),
                   anti_collision_field = 'alt_id')
      )
      
      resultRepo = self$repo$query(V)
      try({
        assert(nrow(resultRepo) == nrow(batch1) + nrow(batch2))
        assert(min(resultRepo$vid) == minVid)     
        assert(max(resultRepo$vid) == minVid + nrow(batch1) + nrow(batch2) - 1)     
      })
      
      try({
        batch = dplyr::bind_rows(batch1, batch2)
        agg = batch %>% dplyr::count(chrom, pos)
        result = self$repo$query(V$where(alt_id != 0))
        assert(nrow(result) == nrow(batch) - nrow(agg))
      })
      
    }
  )
)

# Debugging -------------------------------------------------------------------------------------------------------

connect_to_scidb = function(...) {
  result = scidb::scidbconnect(...)
  options(scidb.aio = TRUE)
  options(scidb.result_size_limit = 1*1024)
  result
}


# Run at user side ------------------------------------------------------------------------------------------------


#' Run tests with a scidb connection
#' @param db a scidb connection returned by calling `scidb::scidbconnect`
#' If not provided, a connection will be created using other params.
#' @param namespace Namespace to run tests in
#' @param host scidb host
#' @param username scidb user name
#' @param password scidb password
#' @param port scidb port default: 8083
#' @param protocol scidb protocol default: 'https'
#' @export
run_tests_with_scidb_connection = function(db = NULL, namespace = 'test_arrayop', create_namespace = FALSE,
                                           host = "localhost",
                                           username = 'scidbadmin',
                                           password = readLines('~/.bms_password'),
                                           port = 8083,
                                           protocol = "https"
                                           ) {
  if(is.null(db)){
    db = connect_to_scidb(host = host, username = username, password = password, port = port, protocol = protocol)
  }
  repo = newRepo(default_namespace = namespace, db = db)
  testClass = ScidbTest$new(repo, namespace)
  testClass$run_tests(create_namespace)
  cat("All tests are done. ")
}
