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
  
  expect_identical(arrayStored$persist(), arrayStored)
  expect_identical(arrayBuildPersisted$is_persistent(), T)  
  expect_identical(arrayBuildPersisted$persist(), arrayBuildPersisted)
})
