context("Repo tests with scidb connection")

`%>%` = dplyr::`%>%`

# arrays loaded from the config file
CfgArrays = repo$load_arrays_from_config(config)
# Get array's raw names (without namespace)
CfgArraysRawNames = sapply(CfgArrays, function(x) gsub("^[^\\.]+\\.", '', x$to_afl()))

# Populate the empty namespace ----
test_that("No arrays in the namespace", {
  arrays = repo$load_arrays_from_scidb_namespace(NS)
  expect_identical(length(arrays), 0L)
})

test_that("Loaded arrays are created in the namespace", {
  expect_identical(length(CfgArrays), length(config$arrays))
  
  # Create arrays manually
  for(arr in CfgArrays){
    repo$execute(sprintf("create array %s", str(arr)))
  }
  
  # Ensure the newly created arrays can be loaded
  scidbCfgArrays = repo$load_arrays_from_scidb_namespace(NS)
  scidbCfgArrays = scidbCfgArrays[CfgArraysRawNames]
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
  
  uploaded = repo$upload_df(df, template, temp = F, use_aio_input = F)
  uploaded2 = repo$upload_df(df, template, temp = T, use_aio_input = T)
  
  templateMatchedDTypes = template$get_field_types(names(df), .raw = T)
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
  df = data.frame(f_str = letters[1:5], f_double = c(3.14, 2.0, NA, 0, -99), f_bool = c(T,NA,F,NA,F), f_int64 = 1:5 * 10.0)
  template = CfgArrays[['template_a']]
  
  uploaded = repo$upload_df(df, template, temp = F)
  filtered = uploaded$where(f_double > 0)
  
  randomName = sprintf('myStoredArray%s', .random_attr_name())
  stored1 = repo$save_as_array(filtered, name = randomName, temp = F, gc = F)
  stored2 = repo$save_as_array(filtered, temp = T)
  filteredDf = dplyr::filter(df, f_double > 0)
  
  result1 = repo$query(stored1, only_attributes = T)
  result2 = repo$query(stored2, only_attributes = T)
  expect_equal(result1, filteredDf)
  expect_equal(result2, filteredDf)
  
  # We can 'overwrite' an existing array
  filtered = uploaded$where(f_double < 0)
  filteredDf = dplyr::filter(df, f_double < 0)
  stored3 = repo$save_as_array(filtered, name = randomName, temp = F, gc = F)
  result3 = repo$query(stored3, only_attributes = T)
  expect_equal(result3, filteredDf)
  repo$execute(afl(stored3 %remove% NULL))
})

# Mutate array content ----

get_mutate_array = function(reset = TRUE){
  target = CfgArrays[['mutate_array']]
  if(reset){
    
  }
  target
}

MutateArray = CfgArrays[['mutate_array']]
DfMutateArray = data.frame(da=0:4, db=10:14,
                                  lower = letters[1:5], upper = LETTERS[1:5],
                                  f_int32 = -5:-1, f_int64 = 1:5 * 10.0,
                                  f_bool = c(T,NA,F,NA,F), f_double = c(3.14, 2.0, NA, 0, -99))

reset_array_with_content = function(target, content, recreate = TRUE) {
  # Reset array
  if(recreate){
    try(repo$execute(afl(target %remove% NULL)), silent = T)
    repo$execute(target$create_array_cmd(target$to_afl()))
  }
  # Upload data 
  repo$execute(
    repo$
      upload_df(content, target)$
      change_schema(target)$
      overwrite(target)
    )
}


test_that("Array content is correctly populated", {
  reset_array_with_content(MutateArray, DfMutateArray)
  
  # Validate the target array
  # sort by lower, needed because scidb doesn't keep the order of inserted data
  dfFromDb = dplyr::arrange(repo$query(MutateArray), lower)
  expect_equal(dfFromDb, DfMutateArray)
})

test_that("Mutate an array by expression", {
  reset_array_with_content(MutateArray, DfMutateArray)
  target = MutateArray
  df = DfMutateArray
  
  mutated = target$where(is_null(f_bool))$
    mutate(list('f_bool'='iif(f_double > 1, true, false)', 'upper'="'mutated'"))
  repo$execute(mutated$update(target))
  dfFromDb = dplyr::arrange(repo$query(target), lower)
  expect_equal(dfFromDb, dplyr::mutate(df, upper=c('A', 'mutated', 'C', 'mutated', 'E'), f_bool=c(T,T,F,F,F)))
})

test_that("Mutate an array by another array", {
  
  mutate_by_one_attr = function(){
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      df %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(lower, upper) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
     MutateArray$
        mutate(mutateDataSource, keys = c('lower'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
  }
  
  mutate_by_two_attrs = function(){
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      df %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(lower, upper) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
     MutateArray$
        mutate(mutateDataSource, keys = c('lower', 'upper'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_one_dim = function(){
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      df %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(da) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
     MutateArray$
        mutate(mutateDataSource, keys = c('da'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_two_dims = function(){
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      df %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(da, db) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
     MutateArray$
        mutate(mutateDataSource, keys = c('da', 'db'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_dim_n_attr = function(){
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      df %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(db, lower) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
     MutateArray$
        mutate(mutateDataSource, keys = c('lower', 'db'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  validate_result = function(){
    dfFromDb = dplyr::arrange(repo$query(MutateArray), lower)
    expect_equal(dfFromDb, df %>% dplyr::mutate(f_int32 = c(1,-4,2,-2,3)))
  }
  # repo$.private$set_meta('debug', T)
  mutate_by_one_attr()
  mutate_by_two_attrs()
  mutate_by_one_dim()
  mutate_by_two_dims()
  mutate_by_dim_n_attr()
  # repo$.private$set_meta('debug', F)
})
