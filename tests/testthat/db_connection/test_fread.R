context("ScidbConnection$fread")

new_temp_file = function() {
  tempfile(pattern = "arrayop_test", tmpdir = "/dev/shm")
}

clean_up_temp_files = function() {
  browser()
  lapply(list.files("/dev/shm", "arrayop_test", full.names = T), file.remove)
}

new_temp_file_with_lines = function(lines) {
  f = new_temp_file()
  writeLines(lines, f)
  f
}



test_that("Test file IO", {
  lines = c("a", "b", "c")
  myFile = new_temp_file_with_lines(lines)
  expect_identical(lines, readLines(myFile))
  expect_identical(file.exists(myFile), T)
})

test_that("explict params", {
  with_sep = function(SEP) {
    f = new_temp_file()
    df = data.frame(fa = letters[1:5], fb = 1:5, da = 1:5)
    data.table::fwrite(df, file = f, sep = SEP)
    
    arrayFromFile = conn$fread(f, header = T, sep = SEP)
    
    expect_equal(data.table::fread(f, data.table = F), df)
    expect_equal(
      conn$fread(f, header = T, sep = SEP)$to_df_attrs(), 
      df
    )
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = names(df))$to_df_attrs(), 
      df
    )
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = c('x','y','z'))$to_df_attrs(), 
      df %>% dplyr::rename(x=fa, y=fb, z=da)
    )
  }
  
  with_sep('\t')
  with_sep(',')
  
})

# cleanup ----
clean_up_temp_files()
