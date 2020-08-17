context("ArrayOp misc")

test_that("if an array is persisted or transient", {
  dataFrame = data.frame(a=1:3, b=letters[1:3])
  arrayStored = conn$array_op_from_uploaded_df(dataFrame)
  arrayBuild = conn$array_op_from_build_literal(dataFrame)
  
  expect_identical(arrayStored$is_persistent(), T)  
  expect_identical(arrayBuild$is_persistent(), F)  
})

test_that("persist arrays so they can be reused", {
  dataFrame = data.frame(a=1:3, b=letters[1:3])
  arrayStored = conn$array_op_from_uploaded_df(dataFrame)
  arrayBuild = conn$array_op_from_build_literal(dataFrame)
  arrayBuildPersisted = arrayBuild$persist()
  arrayBuildPersistedTemp = arrayBuild$persist(.temp = T)
  
  expect_identical(arrayStored$persist(), arrayStored)
  expect_identical(arrayBuildPersisted$is_persistent(), T)  
  expect_identical(arrayBuildPersisted$persist(), arrayBuildPersisted)
  
  expect_identical(arrayBuildPersisted$array_meta_data()$temporary, FALSE)
  expect_identical(arrayBuildPersistedTemp$array_meta_data()$temporary, TRUE)
})

test_that("verify persistent array existence", {
  name = random_array_name()
  arr = conn$create_new_scidb_array(name, "<a:string> [z]")
  
  expect_true(arr$is_persistent())
  expect_true(arr$exists_persistent_array())
  expect_identical(arr$array_meta_data()$name, name)
  
  arr$remove_self()
  
  expect_true(!arr$exists_persistent_array())
})
