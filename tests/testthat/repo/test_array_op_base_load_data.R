context("Test data loading logic with ArrayOpBase class")

newArrayOp = function(...) ArrayOpBase$new(...)

# Not all fields are known/availabe before loading data from source to target
# E.g. a target field should be set to current timestamp
# NOTE: data types are ignored in tests below

Target = newArrayOp('target', c('da', 'db'), c('aa', 'ab'), 
  dtypes = list(da='int64', db='int64', aa='string', ab='int32'))

test_that("Write a dataset ArrayOp to target in redimension mode", {
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'ab'), 
    dtypes = list(x='int64', da='int64', db='int64', aa='string', ab='int32'))
 
  writeOp = ds$reshape(list('da' = 'now()', 'db', 'aa', 'ab'))$write_to(Target)
  assert_afl_equal(writeOp$to_afl(), 
    "insert(redimension(
      project(
        apply(dataset, da, now()), 
        da, db, aa, ab
      ),
    target), target)")
 
  writeOp = ds$reshape(c('da' = 'now()', ds$dims_n_attrs %n% Target$dims_n_attrs))$write_to(Target)
  assert_afl_equal(writeOp$to_afl(), 
    "insert(redimension(
      project(
        apply(dataset, da, now()), 
        da, db, aa, ab
      ),
    target), target)")
})

