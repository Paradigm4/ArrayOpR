context("Scidb V18.x ArrayOp")

# aio_input -------------------------------------------------------------------------------------------------------

test_that("Load array from file", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  for(loaded  in c(
    template$load_file('file_path'),
    template$load_file('file_path', file_headers = c('da', 'db', 'aa', 'ab'))
  )){
    assert_afl_equal(loaded$to_afl(),
      "project(
        apply(
          aio_input('path=file_path', 'num_attributes=4'),
          da, int64(a0), db, int64(a1), aa, a2, ab, double(a3)
        ),
      da, db, aa, ab)")
  }
  assert_afl_equal(
    template$load_file('file_path', file_headers = c('aa', 'ab'))$to_afl(),
    "project(
        apply(
          aio_input('path=file_path', 'num_attributes=2'),
          aa, a0, ab, double(a1)
        ),
    aa, ab)"
  )
  # apply/project'ed fields order follows the template schema, ie. dims + attrs
  assert_afl_equal(
    template$load_file('file_path', file_headers = c('aa', 'db'))$to_afl(),
    "project(
        apply(
          aio_input('path=file_path', 'num_attributes=2'),
          db, int64(a1), aa, a0
        ),
    db, aa)"
  )
})

test_that("Load array from file with skipped file columns", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  loaded = template$load_file('file_path', file_headers = c('skip', 'da', 'db', 'aa', 'ab'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=5'),
        da, int64(a1), db, int64(a2), aa, a3, ab, double(a4)
      ),
    da, db, aa, ab)")
  
  loaded = template$load_file('file_path', file_headers = c('da', 'skip', 'skip', 'db'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a3)
      ),
    da, db)")
  
})

test_that("Load array from file with customized field conversion", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab', 'ac', 'ad'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double', ac='int32', ad='bool'))
  loaded = template$load_file('file_path', 
    field_conversion = list(ab = 'int64(@)+42', ad = 'dcast(@, bool(null))'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=6'),
        da, int64(a0), db, int64(a1), aa, a2, ab, int64(a3)+42, ac, int32(a4), ad, dcast(a5, bool(null))
      ),
    da, db, aa, ab, ac, ad)")
})

test_that("Load array from file with extra settings", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab', 'ac', 'ad'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double', ac='int32', ad='bool'))
  # Pass the delimiter by \\
  loaded = template$load_file('file_path', aio_settings = list(header = 1, attribute_delimiter = '\\t'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=6', 'header=1', 'attribute_delimiter=\\t'),
        da, int64(a0), db, int64(a1), aa, a2, ab, double(a3), ac, int32(a4), ad, bool(a5)
      ),
    da, db, aa, ab, ac, ad)")
  # Pass the delimiter by literal
  loaded = template$load_file('file_path', aio_settings = list(header = 1, attribute_delimiter = '\t'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=6', 'header=1', 'attribute_delimiter=\t'),
        da, int64(a0), db, int64(a1), aa, a2, ab, double(a3), ac, int32(a4), ad, bool(a5)
      ),
    da, db, aa, ab, ac, ad)")
})
