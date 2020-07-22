context("Set array with auto generated fields")


# Shared array schema and data

AutoFields = testNS$create_local_arrayop(
  'auto_fields_array',
  "<f_str:string, f_int32:int32, f_int64:int64> 
      [da=0:*:0:1000; db=0:*:0:1000]"
)
AutoFieldsContent = data.frame(da=1:10, db=1:10, f_str = letters[1:10], f_int32=1:10, f_int64=11:20)

# Tests

test_that("auto increment id", {
  testNS$reset_array_with_content(AutoFields, AutoFieldsContent)
  
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
  testNS$reset_array_with_content(AutoFields, AutoFieldsContent)
  
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
