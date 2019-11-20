context("Test data loading logic with ArrayOpBase class")

newArrayOp = function(...) ArrayOpBase$new(...)

# Not all fields are known/availabe before loading data from source to target
# E.g. a target field should be set to current timestamp
# NOTE: data types are ignored in tests below

Target = newArrayOp('target', c('da', 'db'), c('aa', 'ab'), 
  dtypes = list(da='int64', db='int64', aa='string', ab='int32'))

test_that("Write a dataset ArrayOp to target in redimension mode with missing fields", {
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'ab'), 
    dtypes = list(x='int64', db='int64', aa='string', ab='int32'))
 
  for(writeOp in c(
    ds$reshape(list('da' = 'now()', 'db', 'aa', 'ab'))$write_to(Target)
    # Existing fields can be constructed from the fields of existing ArrayOps
    ,  ds$reshape(c('da' = 'now()', ds$dims_n_attrs %n% Target$dims_n_attrs))$write_to(Target)
    ,  ds$reshape(c('da' = 'now()', Target$dims_n_attrs %-% 'da'))$write_to(Target)
  ))
  {
    assert_afl_equal(writeOp$to_afl(), 
    "insert(redimension(
      project(
        apply(dataset, da, now()), 
        da, db, aa, ab
      ),
    target), target)")
  }
})

test_that("Write a dataset ArrayOp to target in redimension mode with auto-incremented field", {
  # Suppose 'da' is an auto-incremented field of Target
  # Suppose ds.x is a dimension starting from 1, like from the unpack operator
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'da'), 
    dtypes = list(x='int64', db='int64', aa='string', da='int32'))
  
  writeOp = ds$write_to(Target, source_auto_increment = c(x = 3), target_auto_increment = c(ab = 5))
  assert_afl_equal(writeOp$to_afl(), 
    "insert(redimension(
      apply(
        cross_join(
          dataset,
          aggregate(
            target, max(ab) as _max_ab)
        ),
        ab, iif(_max_ab is null, x + 2, _max_ab + x -2)
      )
    ,target), target)")
  
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'ab'), 
    dtypes = list(x='int64', db='int64', aa='string', ab='int32'))
  writeOp = ds$write_to(Target, source_auto_increment = c(x = 0), target_auto_increment = c(da = 1))
  assert_afl_equal(writeOp$to_afl(), 
    "insert(redimension(
      apply(
        cross_join(
          dataset,
          aggregate(
            apply(target, da, da), max(da) as _max_da)
        ),
        da, iif(_max_da is null, x + 1, _max_da + x + 1)
      )
    ,target), target)")
  
  expect_error(ds$write_to(Target, source_auto_increment = c(non = 0), target_auto_increment = c(da = 1)),
    "not exist")
})


