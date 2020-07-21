context("Repo tests with scidb connection")


# Upload data frame to scidb ---- 

test_that("Upload data frame to scidb", {
  df = data.frame(f_str = letters[1:5], f_double = c(3.14, 2.0, NA, 0, -99), f_bool = c(T,NA,F,NA,F), f_int64 = 1:5 * 10.0, 
                  f_datetime = c('2020-03-14 01:23:45', '2000-01-01', '01/01/1999 12:34:56', as.character(Sys.time()), "2020-01-01 3:14:15"))
  # Cannot pass datetime directly from data frame
  # df2 = data.frame(f_str = letters[1:5], f_double = c(3.14, 2.0, NA, 0, -99), f_bool = c(T,NA,F,NA,F), f_int64 = 1:5 * 10.0, 
  #                 f_datetime = Sys.time())
  template = CfgArrays[['template_a']]
  
  uploaded = repo$upload_df(df, template, temp = F, use_aio_input = F)
  # Template can also be a list of data types 
  uploaded2 = repo$upload_df(df, template$get_field_types(), temp = T, use_aio_input = T)
  
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
  
  # Test repo$limit function
  expect_equal(repo$limit(uploaded, 3, only_attributes=TRUE), dfCopy[1:3,])
  expect_equal(repo$limit(uploaded, 3, offset=1, only_attributes=TRUE), 
               data.frame(dfCopy[2:4,], row.names = NULL))
  
  # Validate number of rows
  expect_equal(repo$nrow(uploaded), nrow(df))
  expect_equal(repo$nrow(uploaded2), nrow(df))
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
  repo$execute(afl(stored3 | remove))
})

# Mutate array content ----

MutateArray = CfgArrays[['mutate_array']]
DfMutateArray = data.frame(da=0:4, db=10:14,
                                  lower = letters[1:5], upper = LETTERS[1:5],
                                  f_int32 = -5:-1, f_int64 = 1:5 * 10.0,
                                  f_bool = c(T,NA,F,NA,F), f_double = c(3.14, 2.0, NA, 0, -99))

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
      DfMutateArray %>% dplyr::filter(da %% 2 == 0) %>% dplyr::select(lower, upper) %>% dplyr::mutate(f_int32 = 1:3)
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
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
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
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
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
    reset_array_with_content(MutateArray, DfMutateArray, recreate = F)
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
})


# Set array auto fields ---- 

AutoFields = CfgArrays[['auto_fields_array']]
AutoFieldsContent = data.frame(da=1:10, db=1:10, f_str = letters[1:10], f_int32=1:10, f_int64=11:20)

test_that("auto increment id", {
  reset_array_with_content(AutoFields, AutoFieldsContent)
  
  # Remove some rows
  repo$execute(AutoFields$where(f_int32 > 1 && f_int32 < 5)$overwrite(AutoFields))
  expect_equal(repo$nrow(AutoFields),
               nrow(dplyr::filter(AutoFieldsContent, f_int32 > 1, f_int32 < 5)))
  
  # Update
  source = AutoFields$build_new(data.frame(f_str = letters[5:10]), artificial_field='z')$
    set_auto_fields(AutoFields, source_auto_increment=c(z=0), 
                    target_auto_increment=c(da=1, db=1, f_int32=1, f_int64=11))$
    mutate(list(f_int32="int32(f_int32)")) # auto incremented fields are int64, need to coerce it to int32 for 'f_int32'
  
  repo$execute(source$change_schema(AutoFields)$update(AutoFields))
  expect_equal(dplyr::arrange(repo$query(AutoFields), f_str), 
               data.frame(dplyr::select(AutoFieldsContent[2:10, ], AutoFields$dims_n_attrs), row.names = NULL))
})

test_that("anti-collision field", {
  reset_array_with_content(AutoFields, AutoFieldsContent)
  
  # Remove some rows
  repo$execute(AutoFields$where(f_int32 <= 3)$overwrite(AutoFields))
  expect_equal(repo$nrow(AutoFields),
               nrow(dplyr::filter(AutoFieldsContent, f_int32 <= 3)))
  # da db f_str
  # 1 1 a
  # 2 2 b
  # 3 3 c
  
  # Insert first batch
  source = AutoFields$build_new(data.frame(f_str = letters[4:10], f_int32 = 4:10, da=1:7), artificial_field='z')$
    set_auto_fields(AutoFields, source_auto_increment=c(z=0), 
                    target_auto_increment=c(f_int64=11), anti_collision_field = 'db')
  repo$execute(source$change_schema(AutoFields)$update(AutoFields))
  
  # Validate after 1st insert
  dfFromDb1 = dplyr::arrange(repo$query(AutoFields), f_str)
  dfExpected1 = dplyr::mutate(AutoFieldsContent, da=c(1:3,1:7), db=c(1:3,2:4,0,0,0,0))
  expect_equal(dfFromDb1, dfExpected1)
  
  # Insert new data again
  df = dplyr::select(AutoFieldsContent, -db, -f_int64)
  writeOp = AutoFields$build_new(df, artificial_field = 'z')$
    set_auto_fields(AutoFields, anti_collision_field = 'db', 
                    source_auto_increment=c(z=0), target_auto_increment=c(f_int64=11))$
    change_schema(AutoFields)$
    update(AutoFields)
  repo$execute(writeOp)
  
  # Validate after 2nd insert
  dfFromDb2 = dplyr::arrange(repo$query(AutoFields), f_int64)
  dfExpected2 = data.frame(da = c(dfExpected1$da, AutoFieldsContent$da), 
                           db = c(dfExpected1$db, dfExpected1$db[4:10]+1, rep(0, 3)),
                           f_str = rep(AutoFieldsContent$f_str, 2),
                           f_int32 = c(dfExpected1$f_int32, AutoFieldsContent$f_int32),
                           f_int64 = 11:(nrow(AutoFieldsContent)*2+10))
  expect_equal(dfFromDb2, dfExpected2)
})
