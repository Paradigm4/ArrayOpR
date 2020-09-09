context("ArrayOp: set_auto_fields")

df_equal = function(actual_df, expected_df) {
  expect_equal(
    # actual_df %>% dplyr::arrange(!!! sapply(names(actual_df), as.name)), 
    actual_df %>% dplyr::arrange(f_str), 
    expected_df
  )
}

# Shared array schema and data

schema = conn$array_from_schema("<f_str:string, f_int32:int32, f_int64:int64> [da=0:*:0:1000; db=0:*:0:1000]")
AutoFieldsContent = data.frame(da=1:10, db=1:10, f_str = letters[1:10], f_int32=1:10, f_int64=11:20)

SourceArray = conn$array_from_df(AutoFieldsContent, schema, force_template_schema = T)$
  persist(.temp=T, .gc = F)

SourceEmptyArray = conn$create_array(random_array_name(), schema, .temp = T)

# Tests

test_that("set auto increment fields", {
  build_arr_literal = conn$compile_df(
    data.frame(f_str = LETTERS[1:3]), template = SourceArray,
    skip_scidb_schema_check = T, build_dim_spec = "z"
  )
  # target_auto_increment does not matter if the target array is not empty
  df_equal(
    build_arr_literal$set_auto_fields(
      SourceArray, source_auto_increment=c(z=0), target_auto_increment=c(da=1, db=2, f_int32=3))$
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13, db = 11:13, f_int32=11:13)
  )
  
  # target_auto_increment matters when the target array is empty
  df_equal(
    build_arr_literal$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment=c(z=0),
                      target_auto_increment=c(da=1, db=2, f_int32=3))$
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 1:3, db = 2:4, f_int32=3:5)
  )
  
  # Default params work in commose use cases when 
  # 1. the source array has a single 0-based dimension
  # 2. the target array reference dimesnion(s) are all 0-based
  
  # if no params provided, currently no fields are added
  
  df_equal(
    build_arr_literal$set_auto_fields(SourceArray)$to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13, db = 11:13, f_int32=11:13, f_int64 = 21:23)
  )
  
  df_equal(
    build_arr_literal$set_auto_fields(SourceArray, "z", "da")$to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13)
  )
  
  df_equal(
    build_arr_literal$set_auto_fields(SourceEmptyArray, "z", "da")$to_df(),
    data.frame(f_str = LETTERS[1:3], da = 0:2)
  )
  
  # use source attr instead of dimension (above cases) as the auto increment field
  
  df_equal(
    build_arr_literal$
      mutate("xx" = "z+10")$
      set_auto_fields(SourceArray, c("xx"=10), "da")$
      mutate("xx" = NULL)$ # remove the xx field
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13)
  )
  
  df_equal(
    build_arr_literal$
      mutate("xx" = "z+10")$
      set_auto_fields(SourceArray, c("xx"=10, "z"=0), c("da", "f_int64"))$
      mutate("xx" = NULL)$ # remove the xx field
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13, f_int64 = 21:23)
  )
  
  df_equal(
    build_arr_literal$
      mutate("xx" = "z")$
      set_auto_fields(SourceArray, "xx", "da")$
      mutate("xx" = NULL)$ # remove the xx field
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 11:13)
  )
  
  df_equal(
    build_arr_literal$
      mutate("xx" = "z")$
      set_auto_fields(SourceEmptyArray, "xx", "da")$
      mutate("xx" = NULL)$ # remove the xx field
      to_df(),
    data.frame(f_str = LETTERS[1:3], da = 0:2)
  )
  
})

test_that("set anti-collision fields", {
  # Vanillia case where 'da' has no duplicates in source or target arrays, 
  # but when we want to insert the source data to target array, there will be duplicates
  # so 'db' is incremented to ensure unqiue dimension coordinates
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7))$
      set_auto_fields(SourceArray,
                      anti_collision_field = 'db')$
      to_df(),
    data.frame(da = 1:7, db = 2:8, f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # 'da' has duplicates in both the data source and target arrays
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)))$
      set_auto_fields(SourceArray,
                      anti_collision_field = 'db')$
      to_df(),
    data.frame(da = c(1:3,1:3,1L), db = c(2:4, 3:5, 4), f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # When the target array is empty
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7))$
      set_auto_fields(SourceEmptyArray,
                      anti_collision_field = 'db')$
      to_df(),
    data.frame(da = 1:7, db = 0L, f_str = LETTERS[4:10], f_int32 = 4:10)
  )
  
  # when dimensions 'da' has duplicated values, the anti-collision 'db' dimension should disambiguate
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)))$
      set_auto_fields(SourceEmptyArray,
                      anti_collision_field = 'db')$
      to_df() ,
    data.frame(da = c(1:3,1:3,1L), db = c(0,0,0,1,1,1,2), f_str = LETTERS[4:10], f_int32 = 4:10)
  )
})

test_that("set both auto-incremented fields and anti-collision fields", {
  # Vanillia case where 'da' has no duplicates in source or target arrays, 
  # but when we want to insert the source data to target array, there will be duplicates
  # so 'db' is incremented to ensure unqiue dimension coordinates
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7), artificial_field = 'z')$
      set_auto_fields(SourceArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df(),
    data.frame(da = 1:7, db = 2:8, f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 21:27)
  )
  
  # 'da' has duplicates in both the data source and target arrays
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)), artificial_field = 'z')$
      set_auto_fields(SourceArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df() ,
    data.frame(da = c(1:3,1:3,1L), db = c(2:4, 3:5, 4), f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 21:27)
  )
  
  # When the target array is empty
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7), artificial_field = 'z')$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment = "z",
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df() ,
    data.frame(da = 1:7, db = 0L, f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 121:127)
  )
  
  # when dimensions 'da' has duplicated values, the anti-collision 'db' dimension should disambiguate
  df_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = c(1:3,1:3,1L)), artificial_field = 'z')$
      set_auto_fields(SourceEmptyArray,
                      source_auto_increment = "z",
                      target_auto_increment = c(f_int64=121),
                      anti_collision_field = 'db')$
      to_df() ,
    data.frame(da = c(1:3,1:3,1L), db = c(0,0,0,1,1,1,2), f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 121:127)
  )
})

SourceArray$remove_array()
SourceEmptyArray$remove_array()
