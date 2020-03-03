context("Repo tests with scidb connection")

# arrays loaded from the config file
CfgArrays = repo$load_arrays_from_config(config)

# Populate the empty namespace ----
test_that("No arrays in the namespace", {
  arrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Loaded arrays are created in the namespace", {
  expect_identical(length(CfgArrays), length(config$arrays))
  
  # Create arrays manually
  for(arr in CfgArrays){
    repo$execute(sprintf("create array %s", str(arr)), .raw = T)
  }
  
  # Ensure the newly created arrays can be loaded
  scidbCfgArrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(scidbCfgArrays), length(CfgArrays))
  for(i in 1:length(scidbCfgArrays)){
    fromConfig = CfgArrays[[i]]
    fromScidb = scidbCfgArrays[[i]]
    expect_identical(fromScidb$to_afl(), fromConfig$to_afl())
    # Schema strings from scidb may be upper-cased
    expect_identical(fromScidb$to_schema_str(), fromConfig$to_schema_str())
  }
})

test_that("Get array by raw name", {
  for(arr in CfgArrays){
    fromScidb = repo$load_array_from_scidb(arr$to_afl())
    expect_identical(str(fromScidb), str(arr))
  }
})


# Upload data frame to scidb ---- 

test_that("Upload data frame to scidb", {
  df = data.frame(f_str = letters[1:5], f_double = c(3.14, 2.0, NA, 0, -99), f_bool = c(T,NA,F,NA,F), f_int64 = 1:5 * 10.0, 
                  f_datetime = c('2020-03-14 01:23:45', '2000-01-01', '01/01/1999 12:34:56', as.character(Sys.time()), "2020-01-01 3:14:15"))
  # Cannot pass datetime directly from data frame
  # df2 = data.frame(f_str = letters[1:5], f_double = c(3.14, 2.0, NA, 0, -99), f_bool = c(T,NA,F,NA,F), f_int64 = 1:5 * 10.0, 
  #                 f_datetime = Sys.time())
  template = CfgArrays[['template_a']]
  
  uploaded = repo$upload_df(df, template, use_aio_input = F)
  uploaded2 = repo$upload_df(df, template, use_aio_input = T)
  
  templateMatchedDTypes = template$get_field_types(names(df), .raw=TRUE)
  expect_identical(uploaded$get_field_types(uploaded$attrs), templateMatchedDTypes)
  expect_identical(uploaded2$get_field_types(uploaded2$attrs), templateMatchedDTypes)
  
  # Check array content
  dbdf1 = repo$query(uploaded, only_attributes=TRUE)
  dbdf2 = repo$query(uploaded2, only_attributes=TRUE)
  # R date time conversion is cubersome. We replace it with the scidb parsed values.
  dfCopy = df
  dfCopy$f_datetime = dbdf1[['f_datetime']] 
  expect_equal(dbdf1, dfCopy)
  expect_equal(dbdf2, dfCopy)
})

# Store AFL as a scidb array and return arrayOp ----

test_that("Store AFL as scidb array and return arrayOp", {
  aflStmt = "list('namespaces')"
  arrayOp = repo$save_as_array(aflStmt)
  arrayOp2 = repo$save_as_array(aflStmt, temp = F) # Here we creaet a temporary array
  
  resultFromStmt = repo$query(aflStmt, .raw = T, only_attributes = T)
  resultFromaArrayOp = repo$query(arrayOp, only_attributes = T)
  resultFromaArrayOp2 = repo$query(arrayOp2, only_attributes = T)
  expect_identical(resultFromaArrayOp, resultFromStmt)
  expect_identical(resultFromaArrayOp2, resultFromStmt)
})

