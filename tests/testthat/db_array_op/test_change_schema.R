context("ArrayOp: change schema according to template")

Template = conn$create_new_scidb_array(utility$random_array_name(), "<aa:string, ab:double, ac:int64, ad:int32> [da; db]")

test_that("strict mode requires all fields match", {
  # Template = conn$array_from_schema("Template <aa:string, ab:double, ac:int64, ad:int32> [da; db]")
  
  verify = function(changed){
    expect_identical(changed$dims, Template$dims)
    expect_identical(changed$attrs, Template$attrs)
  }
  
  arr1 = conn$array_op_from_df(
    data.frame(aa='aa', ab=3.14, ac=64, ad=32, da=1, db=2, extra = 'extra'),
    template = "<aa:string, ab:double, ac:int64, ad:int32, da:int64, db:int64, extra:string> "
  )
  # Notice field ac is int32, different than the template
  arr2 = conn$array_op_from_df(
    data.frame(aa='aa', ab=3.14, ac=64, ad=32, da=1, db=2, extra = 'extra'),
    template = "<aa:string, ab:double, ac:int32, ad:int32, da:int64, db:int64, extra:string> "
  )
  
  
  verify(arr1$change_schema(Template)$sync_schema())
  verify(arr2$
           mutate("ac"="int64(ac)")$ # Here we corcerce 'ac' to int64 
           change_schema(Template, .setting = c('cells_per_chunk: 1234'))$sync_schema())
  expect_identical(
    # without sync_schema, the invalid setting is not verified locally in R
    arr1$change_schema(Template, .setting = c('non-setting: 1234'))$dims_n_attrs,
    Template$dims_n_attrs
  )
  expect_error(
    # SciDB will report the invalid setting
    arr1$change_schema(Template, .setting = c('non-setting: 1234'))$sync_schema(), 
    'non-setting'
  )
  
})

# Sync schema ----

test_that("sync_schema will update array schema from scidb only if the schema is not already from scidb", {
  
  # Verify arr$sync_schema does not create a new array_op instance
  verify_already_synced = function(arr){
    expect_identical(arr$sync_schema(), arr)
  }
  verify_not_synced = function(arr){
    expect_true(!identical(arr$sync_schema(), arr))
  }
  
  verify_already_synced(conn$array_from_afl("list()"))
  
  verify_already_synced(conn$array_op_from_df(data.frame(a=1,b='B')))
  verify_already_synced(conn$array_op_from_df(data.frame(a=1,b='B'), skip_scidb_schema_check = T)$sync_schema())
  verify_not_synced(conn$array_op_from_df(data.frame(a=1,b='B'), skip_scidb_schema_check = T))
  
})

Template$remove_self()
