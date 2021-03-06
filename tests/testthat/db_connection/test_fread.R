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
      conn$fread(f, header = T, sep = SEP)$to_df(), 
      df
    )
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = names(df))$to_df(), 
      df
    )
    # skip the header row; set column names explicitly
    expect_equal(
      conn$fread(f, header = T, sep = SEP, col.names = c('x','y','z'))$to_df(), 
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
    conn$fread(f, header = F, col.names = names(df))$to_df(), 
    df
  )
  expect_equal(
    conn$fread(f, header = F)$to_df(), 
    df %>% dplyr::rename(V1=fa, V2=fb, V3=da)
  )
  expect_equal(
    conn$fread(f, header = F, col.names = c('x', 'y', 'z'))$to_df(), 
    df %>% dplyr::rename(x=fa, y=fb, z=da)
  )
  # data.table auto generates column names: V1, V2, ...
  expect_equal(
    conn$fread(f, header = F, col.names = NULL)$to_df(), 
    df %>% dplyr::rename(V1=fa, V2=fb, V3=da)
  )
})

test_that("explicit template; with file header; column mapping", {
  f = new_temp_file()
  df = data.frame(fa = letters[1:5], fb = 1:5 * 3.14, da = 1:5)
  data.table::fwrite(df, file = f, sep = '\t')
  
  # conn$fread returns array_op with fields order by template's dimensions and attributes that match file's col.names
  expect_equal(
    conn$fread(f, template = conn$array_from_schema("<fa:string, fb:double> [da]"), header = T)$to_df(), 
    df %>% dplyr::select(da, fa, fb)
  )
  expect_equal(
    conn$fread(f, template = conn$array_from_schema("<fa:string, fb:double> [da]"), header = T, col.name=names(df))$to_df(), 
    df %>% dplyr::select(da, fa, fb)
  )
  
  # As in other ScidbConnection methods, the template param can be array_op or a schema string
  
  # template's non-matching attr/dim are ignored
  expect_equal(
    # template is a list, equivalent to "<extra_attr:string, fa:string, fb:double, da:int64, extra:int64> []"
    conn$fread(f, template = list("extra_attr" = "string", fa="string", fb="double", da="int64", extra="int64"), header = T)$to_df(), 
    df %>% dplyr::select(fa, fb, da)
  )
  
  # 'fa' column in file does not match any template fields. So it's ignored
  expect_equal(
    conn$fread(f, template = "<extra_attr:string, fb:double> [da;extra_dim]", header = T)$to_df(), 
    df %>% dplyr::select(da, fb)
  )
  
})

test_that("explicit template; without file header; column mapping", {
  f = new_temp_file()
  df = data.frame(da = 1:5, fa = letters[1:5], fb = 1:5 * 3.14)
  data.table::fwrite(df, file = f, sep = '\t', col.names = F)
  
  # When header =F && col.names = NULL, col.names is assumed to be the template's dims_n_attrs

  expect_equal(
    conn$fread(f, template = "<fa:string, fb:double> [da]", header = F)$to_df(), 
    df %>% dplyr::select(da, fa, fb)
  )
  expect_equal(
    conn$fread(f, template = "<extra:bool, da:int32, fa:string, fb:double>", header = F, col.names = names(df))$to_df(),
    df
  )
  expect_equal(
    # when file has more columns than template's fields, the extra columns are not loaded
    conn$fread(f, template = "<extra:bool, fa:string, fb:double> [i]", header = F, col.names = names(df))$to_df(), 
    df %>% dplyr::select(fa, fb)
  )
})

test_that("field types and conversion", {
  f = new_temp_file()
  df = data.frame(da = 1:5, f_str = letters[1:5], 
                  f_double = .strsplit("1.1 nonsense 3.4 4.5 5.6"),
                  f_i32 = .strsplit("11 12 nonsense 14 non"), 
                  f_i64 = .strsplit("21 non 23 non 25"),
                  f_bool = .strsplit("T F true false unknown")
                  )
  convertedDf = df %>% dplyr::mutate(
      f_double = suppressWarnings(as.numeric(f_double)), 
      f_i32 = suppressWarnings(as.integer(f_i32)), 
      f_i64 = suppressWarnings(as.integer(f_i64)),
      f_bool = suppressWarnings(as.logical(f_bool))
    )
  data.table::fwrite(df, file = f, sep = '\t')
  
  # When header =F && col.names = NULL, col.names is assumed to be the template's dims_n_attrs
  
  expect_equal(
    conn$fread(f, 
               template = "<f_str:string, f_double:double, f_i32:int32, f_i64:int64, f_bool:bool> [da]", 
               header = T,
               mutate_fields = list(
                 'f_double' = 'dcast(@, double(null))',
                 'f_i32' = 'dcast(@, int32(null))',
                 'f_i64' = 'dcast(@, int64(null))',
                 'f_bool' = 'dcast(@, bool(null))'
                 ),
               )$to_df(), 
    convertedDf
  )
  expect_equal(
    conn$fread(f, 
               template = "<f_str:string, f_double:double, f_i32:int32, f_i64:int64, f_bool:bool> [da]", 
               header = T,
               auto_dcast = T,
               )$to_df(), 
    convertedDf
  )
  expect_equal(
    conn$fread(f, 
               template = "<f_str:string, f_double:double, f_i32:int32, f_i64:int64, f_bool:bool> [da]", 
               header = T,
               auto_dcast = T,
               mutate_fields = list('f_i32'="dcast(@, int32(null)) + 123",
                                       'f_str'="iif(@ = 'a', 'A', iif(@ = 'd', 'D', @))")
               )$to_df(), 
    convertedDf %>% dplyr::mutate(
      f_i32 = f_i32 + 123,
      f_str = .strsplit("A b c D e"))
  )
  
})

test_that("multiple file paths", {
  numInstances = nrow(conn$query("list('instances')"))
  if(numInstances > 1){
    f1 = new_temp_file()
    f2 = new_temp_file()
    
    df1 = data.frame(da = 1:5, fa = letters[1:5], fb = 1:5 * 3.14)
    df2 = data.frame(da = 6:10, fa = letters[6:10], fb = 6:10 * 3.14)
    
    data.table::fwrite(df1, file = f1, sep = '\t', col.names = F)
    data.table::fwrite(df2, file = f2, sep = '\t', col.names = F)
    
    arr = conn$fread(file_path = c(f1, f2), header = F, col.names = names(df1), instances = 0:1)
    
    expect_equal(
      arr$to_df() %>% dplyr::arrange(da),
      dplyr::bind_rows(df1, df2)
    )
  }
})

test_that("extra aio settings", {
  f1 = new_temp_file()
  df1 = data.frame(da = 1:5, fa = letters[1:5], fb = 1:5 * 3.14)
  data.table::fwrite(df1, file = f1, sep = '\t', col.names = F)
  
  arr = conn$fread(file_path = f1, header = F, col.names = names(df1), 
                   .aio_settings = list(chunk_size = 123456))
  expect_match(arr$to_afl(), "123456")
  expect_match(arr$to_afl(), "chunk_size")
  expect_equal(arr$to_df(),df1)
})

# cleanup ----
clean_up_temp_files()
