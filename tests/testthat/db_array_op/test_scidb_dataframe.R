context("Test Scidb DataFrame")

test_that("scidb data frame", {
  dfArray = conn$create_array(dbutils$random_array_name(), "<a:string, b:int32>")
  expect_identical(dfArray$array_meta_data()$distribution, "dataframe")
  
  dataContent = data.frame(a = letters[1:2], b = 1:2)
  uploaded = conn$upload_df(dataContent, dfArray, .temp = T, .gc = F)
  conn$execute(afl(
    uploaded$drop_dims("flatten")$transmute(.dots = as.list(uploaded$attrs)) | append(dfArray)
  ))
  
  expect_equal(dfArray$to_df(), dataContent)
  
  # Scidb does not seem to support insert data from build'ed array
  # buildDf = conn$array_from_df(dataContent, template = "<a:string, b:int32>")
  
  dfArray$remove_self()
  uploaded$remove_self()
})
