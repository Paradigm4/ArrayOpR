context("ScidbConnection$fread")

new_temp_file = function() {
  tempfile(pattern = "arrayop_test", tmpdir = "/dev/shm")
}

clean_up_temp_files = function() {
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

test_that("file with header row; no template", {
  with_sep = function(SEP) {
    f = new_temp_file()
    df = data.frame(fa = letters[1:5], fb = 1:5 * 3.14, da = 1:5)
    data.table::fwrite(df, file = f, sep = SEP)
    
    expect_equal(data.table::fread(f, data.table = F), df)
    expect_equal(
      conn$fread(f, header = T, sep = SEP)$to_df_attrs(), 
      df
    )
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = names(df))$to_df_attrs(), 
      df
    )
    # skip the header row; set column names explicitly
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = c('x','y','z'))$to_df_attrs(), 
      df %>% dplyr::rename(x=fa, y=fb, z=da)
    )
  }
  
  with_sep('\t')
  with_sep(',')
})

test_that("file without header row; no template", {
  f = new_temp_file()
  df = data.frame(fa = letters[1:5], fb = 1:5 * 3.14, da = 1:5)
  data.table::fwrite(df, file = f, sep = '\t', col.names = F)
  
  # conn$fread has a default param sep = '\t'
  expect_equal(
    conn$fread(f, header = F, col.names = names(df))$to_df_attrs(), 
    df
  )
  expect_equal(
    conn$fread(f, header = F)$to_df_attrs(), 
    df %>% dplyr::rename(V1=fa, V2=fb, V3=da)
  )
  expect_equal(
    conn$fread(f, header = F, col.names = c('x', 'y', 'z'))$to_df_attrs(), 
    df %>% dplyr::rename(x=fa, y=fb, z=da)
  )
  # data.table auto generates column names: V1, V2, ...
  expect_equal(
    conn$fread(f, header = F, col.names = NULL)$to_df_attrs(), 
    df %>% dplyr::rename(V1=fa, V2=fb, V3=da)
  )
})

test_that("explicit template; with file header; column mapping", {
  f = new_temp_file()
  df = data.frame(fa = letters[1:5], fb = 1:5 * 3.14, da = 1:5)
  data.table::fwrite(df, file = f, sep = '\t')
  
  # conn$fread returns array_op with fields order by template's dimensions and attributes that match file's col.names
  expect_equal(
    conn$fread(f, template = conn$array_op_from_schema_str("<fa:string, fb:double> [da]"), header = T)$to_df_attrs(), 
    df %>% dplyr::select(da, fa, fb)
  )
  expect_equal(
    conn$fread(f, template = conn$array_op_from_schema_str("<fa:string, fb:double> [da]"), header = T, col.name=names(df))$to_df_attrs(), 
    df %>% dplyr::select(da, fa, fb)
  )
  
  # As in other ScidbConnection methods, the template param can be array_op or a schema string
  
  # template's non-matching attr/dim are ignored
  expect_equal(
    # template is a list, equivalent to "<extra_attr:string, fa:string, fb:double, da:int64, extra:int64> []"
    conn$fread(f, template = list("extra_attr" = "string", fa="string", fb="double", da="int64", extra="int64"), header = T)$to_df_attrs(), 
    df %>% dplyr::select(fa, fb, da)
  )
  
  # 'fa' column in file does not match any template fields. So it's ignored
  expect_equal(
    conn$fread(f, template = "<extra_attr:string, fb:double> [da;extra_dim]", header = T)$to_df_attrs(), 
    df %>% dplyr::select(da, fb)
  )
  
})

test_that("explicit template; without file header; column mapping", {
  f = new_temp_file()
  df = data.frame(da = 1:5, fa = letters[1:5], fb = 1:5 * 3.14)
  data.table::fwrite(df, file = f, sep = '\t', col.names = F)
  
  # When header =F && col.names = NULL, col.names is assumed to be the template's dims_n_attrs

  expect_equal(
    conn$fread(f, template = "<fa:string, fb:double> [da]", header = F)$to_df_attrs(), 
    df %>% dplyr::select(da, fa, fb)
  )
  expect_equal(
    #todo: if array schema string doesn't have a dimension, an error will occur.
    conn$fread(f, template = "<extra:bool, da:int32, fa:string, fb:double> [i]", header = F, col.names = names(df))$to_df_attrs(), 
    df
  )
})

# cleanup ----
clean_up_temp_files()
