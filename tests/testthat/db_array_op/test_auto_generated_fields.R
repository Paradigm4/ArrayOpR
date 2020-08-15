context("Set array with auto generated fields")


# Shared array schema and data

# AutoFields = testNS$create_local_arrayop(
#   'auto_fields_array',
#   "<f_str:string, f_int32:int32, f_int64:int64> 
#       [da=0:*:0:1000; db=0:*:0:1000]"
# )

# print("======================debug================")
# browser()
# 

conn = get_default_connection()

schema = conn$array_op_from_schema_str("<f_str:string, f_int32:int32, f_int64:int64> [da=0:*:0:1000; db=0:*:0:1000]")
AutoFieldsContent = data.frame(da=1:10, db=1:10, f_str = letters[1:10], f_int32=1:10, f_int64=11:20)

SourceArray = conn$array_op_from_df(AutoFieldsContent, schema)$
  change_schema(schema)$
  persist(.temp=T)

sourceEmptyName = utility$random_array_name()
conn$execute(glue::glue("create array {sourceEmptyName} {schema$to_schema_str()}"))

SourceEmptyArray = conn$array_op_from_name(sourceEmptyName)

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

test_that("set both auto-incremented fields and anti-collision fields", {
  expect_equal(
    SourceArray$
      build_new(data.frame(f_str = LETTERS[4:10], f_int32 = 4:10, da = 1:7), artificial_field = 'z')$
      set_auto_fields(SourceArray,
                      source_auto_increment = c(z=0),
                      target_auto_increment = c(f_int64=1234),
                      anti_collision_field = 'db')$
      to_df_attrs(),
    data.frame(da = 1:7, db = 2:8, f_str = LETTERS[4:10], f_int32 = 4:10, f_int64 = 21:27)
  )
})

SourceArray$remove_self()
SourceEmptyArray$remove_self()


# stop("intentional")
# 
# test_that("anti-collision field", {
#   testNS$reset_array_with_content(AutoFields, AutoFieldsContent)
#   
#   # Remove some rows
#   AutoFields$filter(f_int32 <= 3)$overwrite(AutoFields)$execute()
#   expect_equal(AutoFields$row_count(),
#                nrow(dplyr::filter(AutoFieldsContent, f_int32 <= 3)))
#   # da db f_str
#   # 1 1 a
#   # 2 2 b
#   # 3 3 c
#   
#   # Insert first batch
#   source = AutoFields$build_new(data.frame(f_str = letters[4:10], f_int32 = 4:10, da=1:7), artificial_field='z')$
#     set_auto_fields(AutoFields, source_auto_increment=c(z=0), 
#                     target_auto_increment=c(f_int64=11), anti_collision_field = 'db')
#   source$change_schema(AutoFields)$update(AutoFields)$execute()
#   
#   # Validate after 1st insert
#   dfFromDb1 = dplyr::arrange(AutoFields$to_df(), f_str)
#   dfExpected1 = dplyr::mutate(AutoFieldsContent, da=c(1:3,1:7), db=c(1:3,2:4,0,0,0,0))
#   expect_equal(dfFromDb1, dfExpected1)
#   
#   # Insert new data again
#   df = dplyr::select(AutoFieldsContent, -db, -f_int64)
#   writeOp = AutoFields$build_new(df, artificial_field = 'z')$
#     set_auto_fields(AutoFields, anti_collision_field = 'db', 
#                     source_auto_increment=c(z=0), target_auto_increment=c(f_int64=11))$
#     change_schema(AutoFields)$
#     update(AutoFields)
#   writeOp$execute()
#   
#   # Validate after 2nd insert
#   dfFromDb2 = dplyr::arrange(AutoFields$to_df(), f_int64)
#   dfExpected2 = data.frame(da = c(dfExpected1$da, AutoFieldsContent$da), 
#                            db = c(dfExpected1$db, dfExpected1$db[4:10]+1, rep(0, 3)),
#                            f_str = rep(AutoFieldsContent$f_str, 2),
#                            f_int32 = c(dfExpected1$f_int32, AutoFieldsContent$f_int32),
#                            f_int64 = 11:(nrow(AutoFieldsContent)*2+10))
#   expect_equal(dfFromDb2, dfExpected2)
# })
# 
# # cleanup ----
# AutoFields$remove_self()
