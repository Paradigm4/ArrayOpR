context("Scidb V18.x ArrayOp")

# aio_input -------------------------------------------------------------------------------------------------------

test_that("Load array from file", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  loaded = template$load_file('file_path')
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a1), aa, a2, ab, double(a3)
      ),
    da, db, aa, ab)")
})

test_that("Load array from file with skipped file columns", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  loaded = template$load_file('file_path', skip_cols = 0)
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a1), db, int64(a2), aa, a3, ab, double(a4)
      ),
    da, db, aa, ab)")
  loaded = template$load_file('file_path', skip_cols = c(1, 2))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a3), aa, a4, ab, double(a5)
      ),
    da, db, aa, ab)")
  loaded = template$load_file('file_path', skip_cols = c(1, 3))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a2), aa, a4, ab, double(a5)
      ),
    da, db, aa, ab)")
  loaded = template$load_file('file_path', skip_cols = c(1, 3, 5))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a2), aa, a4, ab, double(a6)
      ),
    da, db, aa, ab)")
  loaded = template$load_file('file_path', skip_cols = c(1, 3, 6))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a0), db, int64(a2), aa, a4, ab, double(a5)
      ),
    da, db, aa, ab)")
  loaded = template$load_file('file_path', skip_cols = c(0, 3))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('path=file_path', 'num_attributes=4'),
        da, int64(a1), db, int64(a2), aa, a4, ab, double(a5)
      ),
    da, db, aa, ab)")
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
