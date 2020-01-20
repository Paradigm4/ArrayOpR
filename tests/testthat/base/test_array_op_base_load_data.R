context("Test data loading logic with ArrayOpBase class")

newArrayOp = function(...) ArrayOpBase$new(...)


# Simple cases of write data from source to target ArrayOp --------------------------------------------------------

# Target ArrayOp is the one we want to write data to
Target = newArrayOp('target', c('da', 'db'), c('aa', 'ab'), 
  dtypes = list(da='int64', db='int64', aa='string', ab='int32'))

test_that("Write a dataset ArrayOp to target", {
  ds = newArrayOp('dataset', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa='string', ab='int32'))
  writeOp = ds$write_to(Target, force_redimension=F)
  assert_afl_equal(writeOp$to_afl(), "insert(dataset, target)")
  writeOp = ds$write_to(Target, append = F, force_redimension=F)
  assert_afl_equal(writeOp$to_afl(), "store(dataset, target)")
})

test_that("Field names do not matter as long as the field data types match target", {
  for(ds in c(
    newArrayOp('dataset', c('w', 'x'), c('y', 'z'), dtypes = list(w='int64', x='int64', y='string', z='int32'))
    ,newArrayOp('dataset', c('z', 'x'), c('y', 'w'), dtypes = list(z='int64', x='int64', y='string', w='int32'))
  )){
    writeOp = ds$write_to(Target, force_redimension=F)
    assert_afl_equal(writeOp$to_afl(), "insert(dataset, target)")
  }
})

test_that("Force redimension the source even if data types matches", {
  for(ds in c(
    newArrayOp('dataset', c('w', 'x'), c('y', 'z'), dtypes = list(w='int64', x='int64', y='string', z='int32'))
    ,newArrayOp('dataset', c('z', 'x'), c('y', 'w'), dtypes = list(z='int64', x='int64', y='string', w='int32'))
  )){
    # Since redimension requires field name matches, a 'fields not found' error is thrown
    expect_error(ds$write_to(Target, force_redimension = T), 'not found')
  }
})

# If data types do not exactly match, 'redimension' is used

test_that("Write a dataset ArrayOp to target in redimension mode", {
  for(ds in c(
    newArrayOp('dataset', 'x', c('da', 'db', 'aa', 'ab'), dtypes = list(x='int64', da='int64', db='int64', aa='string', ab='int32'))
    ,newArrayOp('dataset', 'x', c('da', 'db', 'aa', 'ab'))
    ,newArrayOp('dataset', 'x', c('extra_field_is_ok', 'da', 'db', 'aa', 'ab'))
  )){
    writeOp = ds$write_to(Target)
    assert_afl_equal(writeOp$to_afl(), "insert(redimension(dataset, target), target)")
    writeOp = ds$write_to(Target, append = F)
    assert_afl_equal(writeOp$to_afl(), "store(redimension(dataset, target), target)")
  }
})

test_that("All target fields must be present in the source by matching names in Redimension mode ", {
  expect_error(
    newArrayOp('dataset', 'x', c('db', 'aa', 'ab'), 
      dtypes = list(x='int64', db='int64', aa='string', ab='int32'))$write_to(Target), "not found")
})



# Load from data source that misses target fields -----------------------------------------------------------------


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


