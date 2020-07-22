context("Mutate array content/cells")


MutateArray = testNS$create_local_arrayop(
  'mutate_array',
  "<lower:string COMPRESSION 'zlib', upper:string, f_int32:int32, f_int64:int64, f_bool: bool, f_double: double> 
      [da=0:*:0:*; db=0:*:0:*]"
)

DfMutateArray = data.frame(
  da=0:4, db=10:14,
  lower = letters[1:5], 
  upper = LETTERS[1:5],
  f_int32 = -5:-1, 
  f_int64 = 1:5 * 10.0, 
  f_bool = c(T,NA,F,NA,F), 
  f_double = c(3.14, 2.0, NA, 0, -99)
)


test_that("Array content is correctly populated", {
  testNS$reset_array_with_content(MutateArray, DfMutateArray)
  
  # Validate the target array
  # sort by lower, needed because scidb doesn't keep the order of inserted data
  dfFromDb = dplyr::arrange(repo$query(MutateArray), lower)
  expect_equal(dfFromDb, DfMutateArray)
})

test_that("Mutate an array by expression", {
  testNS$reset_array_with_content(MutateArray, DfMutateArray)
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
    testNS$reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(lower, upper) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
      MutateArray$
        mutate(mutateDataSource, keys = c('lower'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
  }
  
  mutate_by_two_attrs = function(){
    testNS$reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(lower, upper) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
      MutateArray$
        mutate(mutateDataSource, keys = c('lower', 'upper'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_one_dim = function(){
    testNS$reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(da) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
      MutateArray$
        mutate(mutateDataSource, keys = c('da'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_two_dims = function(){
    testNS$reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(da, db) %>% dplyr::mutate(f_int32 = 1:3)
    )
    repo$execute(
      MutateArray$
        mutate(mutateDataSource, keys = c('da', 'db'), updated_fields = 'f_int32')$
        update(MutateArray)
    )
    validate_result()
  }
  
  mutate_by_dim_n_attr = function(){
    testNS$reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
    mutateDataSource = MutateArray$build_new(
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(db, lower) %>% dplyr::mutate(f_int32 = 1:3)
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
    expect_equal(dfFromDb, DfMutateArray %>% dplyr::mutate(f_int32 = c(1,-4,2,-2,3)))
  }
  
  mutate_by_one_attr()
  mutate_by_two_attrs()
  mutate_by_one_dim()
  mutate_by_two_dims()
  mutate_by_dim_n_attr()
  
  testNS$cleanup_after_each_test()
})

