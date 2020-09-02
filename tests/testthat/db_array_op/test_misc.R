context("ArrayOp misc")


# Persistent vs. Transient array_op ----

test_that("if an array is persisted or transient", {
  dataFrame = data.frame(a=1:3, b=letters[1:3])
  arrayStored = conn$upload_df(dataFrame)
  arrayBuild = conn$compile_df(dataFrame)
  
  expect_identical(arrayStored$is_persistent(), T)  
  expect_identical(arrayBuild$is_persistent(), F)  
})

test_that("persist arrays so they can be reused", {
  dataFrame = data.frame(a=1:3, b=letters[1:3])
  arrayStored = conn$upload_df(dataFrame)
  arrayBuild = conn$compile_df(dataFrame)
  arrayBuildPersisted = arrayBuild$persist(.gc = F)
  arrayBuildPersistedTemp = arrayBuild$persist(.temp = T, .gc = F)
  
  expect_identical(arrayStored$persist(), arrayStored)
  expect_identical(arrayBuildPersisted$is_persistent(), T)  
  expect_identical(arrayBuildPersisted$persist(), arrayBuildPersisted)
  
  expect_identical(arrayBuildPersisted$array_meta_data()$temporary, FALSE)
  expect_identical(arrayBuildPersistedTemp$array_meta_data()$temporary, TRUE)
  
  arrayBuildPersisted$remove_self()
  arrayBuildPersistedTemp$remove_self()
})

test_that("verify persistent array existence", {
  name = random_array_name()
  arr = conn$create_array(name, "<a:string> [z]")
  
  expect_true(arr$is_persistent())
  expect_true(arr$exists_persistent_array())
  expect_identical(arr$array_meta_data()$name, name)
  
  arr$remove_self()
  
  expect_true(!arr$exists_persistent_array())
})

# drop_dims ---- 

# both 'unpack' and 'flatten' modes will place old dimensions in front of existing attributes
test_that("Drop dims", {
  DataContent = data.frame(da = 1:3, db = 11:13, fa = letters[1:3], fb = 3.14 * 1:3)
  RefArray = conn$
    array_from_df(DataContent, template = "<fa:string, fb:double> [da; db]", force_template_schema = T)$
    persist(.gc = F)
  
  expect_identical(RefArray$dims, c('da', 'db'))
  
  verify = function(dropped, patterns = NULL) {
    expect_identical(dropped$attrs, RefArray$dims_n_attrs)
    expect_equal(dropped$to_df() %>% dplyr::arrange(da), DataContent)
    if(!is.null(patterns)) {
      sapply(patterns, function(x) expect_match(dropped$to_afl(), x))
    }
  }
  
  verify(RefArray$drop_dims())
  verify(RefArray$drop_dims(mode = 'unpack'))
  verify(RefArray$drop_dims(mode = 'unpack', .unpack_dim = 'zz'), 'zz')
  verify(RefArray$drop_dims(mode = 'unpack', .unpack_dim = 'zz', .chunk_size = 123), c('zz', '123'))
  
  verify(RefArray$drop_dims(mode = 'flatten'))
  verify(RefArray$drop_dims(mode = 'flatten', .chunk_size = 123), '123')
  
  RefArray$remove_self()
})

# spawn ----

test_that("no field data type requirement for locally created arrays", {
  template = conn$array_from_schema("<fa:string, fb:int32, fc:bool> [da;db]")
  a = template$spawn(excluded = c('da', 'fa'), added = 'extra', renamed = list(fc='fc_re'))
  
  expect_identical(a$dims, c('db'))
  expect_identical(a$attrs, c('fb','fc_re', 'extra'))
  
})
