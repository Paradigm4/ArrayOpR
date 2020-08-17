context("Set array with auto generated fields")


# Shared array schema and data

schema = conn$array_op_from_schema_str("<f_str:string, f_int32:int32, f_int64:int64> [da=0:*:0:1000; db=0:*:0:1000]")
AutoFieldsContent = data.frame(da=1:10, db=1:10, f_str = letters[1:10], f_int32=1:10, f_int64=11:20)

SourceArray = conn$array_op_from_df(AutoFieldsContent, schema)$
  change_schema(schema)$
  persist(.temp=T, .gc = F)

SourceEmptyArray = conn$create_new_scidb_array(random_array_name(), schema, .temp = T)

# Tests

test_that("set auto increment fields", {
  # target_auto_increment does not matter if the target array is not empty
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[1:3]), artificial_field='z')$
      set_auto_fields(SourceArray,
                      source_auto_increment=c(z=0),
                      target_auto_increment=c(da=1, db=2, f_int32=3))$
      to_df_attrs(),
    data.frame(f_str = LETTERS[1:3], da = 11:13, db = 11:13, f_int32=11:13)
  )
  
  # target_auto_increment matters when the target array is empty
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[1:3]), artificial_field='z')$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment=c(z=0),
                      target_auto_increment=c(da=1, db=2, f_int32=3))$
      to_df_attrs(),
    data.frame(f_str = LETTERS[1:3], da = 1:3, db = 2:4, f_int32=3:5)
  )
})

test_that("set anti-collision fields", {
  # Vanillia case where 'da' has no duplicates in source or target arrays, 
  # but when we want to insert the source data to target array, there will be duplicates
  # so 'db' is incremented to ensure unqiue dimension coordinates
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7))$
      set_auto_fields(SourceArray,
                      anti_collision_field = 'db')$
      to_df_attrs(),
    data.frame(da = 1:7, db = 2:8, f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # 'da' has duplicates in both the data source and target arrays
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)))$
      set_auto_fields(SourceArray,
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = c(1:3,1:3,1L), db = c(2:4, 3:5, 4), f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # When the target array is empty
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7))$
      set_auto_fields(SourceEmptyArray,
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = 1:7, db = 0L, f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # when dimensions 'da' has duplicated values, the anti-collision 'db' dimension should disambiguate
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)))$
      set_auto_fields(SourceEmptyArray,
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = c(1:3,1:3,1L), db = c(0,0,0,1,1,1,2), f_str = LETTERS[4:10], f_int32 = 4:10)
  )
})

test_that("set both auto-incremented fields and anti-collision fields", {
  # Vanillia case where 'da' has no duplicates in source or target arrays, 
  # but when we want to insert the source data to target array, there will be duplicates
  # so 'db' is incremented to ensure unqiue dimension coordinates
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7), artificial_field = 'z')$
      set_auto_fields(SourceArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df_attrs(),
    data.frame(da = 1:7, db = 2:8, f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 21:27)
  )
  
  # 'da' has duplicates in both the data source and target arrays
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)), artificial_field = 'z')$
      set_auto_fields(SourceArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = c(1:3,1:3,1L), db = c(2:4, 3:5, 4), f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 21:27)
  )
  
  # When the target array is empty
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7), artificial_field = 'z')$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = 1:7, db = 0L, f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 121:127)
  )
  
  # when dimensions 'da' has duplicated values, the anti-collision 'db' dimension should disambiguate
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)), artificial_field = 'z')$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df_attrs() %>% dplyr::arrange(f_str),
    data.frame(da = c(1:3,1:3,1L), db = c(0,0,0,1,1,1,2), f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 121:127)
  )
})

SourceArray$remove_self()
SourceEmptyArray$remove_self()
