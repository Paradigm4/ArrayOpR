context("Test JoinOp")

# Join operands. Acronyms are left/right-dimension/attr-filedName.
# E.g. lda: left dimension 'a', rac: right attribute 'c'
# Note that the 'on_right' keys will be omitted from join results per equi_join V18 implementation.

arrL =  AnyArrayOp('L', c('lda', 'ldb', 'ldc', 'ldd'), c('laa', 'lab', 'lac', 'lad'))
arrR = AnyArrayOp('R', c('rda', 'rdb', 'rdc', 'rdd'), c('raa', 'rab', 'rac', 'rad'))


# Join on array attributes ----------------------------------------------------------------------------------------

# NOTE: JoinOp ignores all 'on_right' fields even they are 'selected' in 'right' operand
# because equi_join only keeps the 'on_left' fields
#
test_that("Join two arrays both on selected attributes", {
  left = SubsetOp(arrL, selected_fields = c('laa'))
  right = SubsetOp(arrR, selected_fields = c('raa'))
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa')

  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa),
    project(R, raa),
      'left_names=laa', 'right_names=raa'
  ), laa)
  ")
  expect_identical(joinOp$get_field_names(.SEL), 'laa')
  expect_identical(joinOp$get_field_names(.ATR), 'laa')
  expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_types(c('laa', 'raa')), list(laa = 'dt_laa'))
})

test_that("Multiple join fields", {
  left = SubsetOp(arrL, selected_fields = c('laa', 'lab'))
  right = SubsetOp(arrR, selected_fields = c('raa', 'rac'))
  joinOp = JoinOp(left, right,
    on_left = c('laa', 'lab'), on_right = c('raa', 'rac'))

  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa, lab),
    project(R, raa, rac),
      'left_names=laa,lab', 'right_names=raa,rac'
  ), laa, lab)
  ")
  expect_identical(joinOp$get_field_names(.SEL), c('laa', 'lab'))
  expect_identical(joinOp$get_field_names(.ATR), c('laa', 'lab'))
  expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_types(c('laa', 'lab', 'raa', 'rac')), list(laa = 'dt_laa', lab = 'dt_lab'))
})

# What if no fields are selected for an operand

test_that("No selected fields in one operand (Right-hand)", {
  left = SubsetOp(arrL, selected_fields = 'laa')
  right = arrR
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa),
      R,
      'left_names=laa', 'right_names=raa'
    ),
    laa
  )")

  # Select L attr and join on L dimension
  left = SubsetOp(arrL, selected_fields = 'laa')
  right = arrR
  joinOp = JoinOp(left, right, on_left = 'lda', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa),
      R,
      'left_names=lda', 'right_names=raa'
    ),
    laa
  )")

  # Select L dimension and join on L attribute
  left = SubsetOp(arrL, selected_fields = c('lda', 'laa'))
  right = arrR
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project( apply(L, lda, lda), lda, laa),
      R,
      'left_names=laa', 'right_names=raa'
    ),
    lda, laa
  )")
})



# on_right keys are omitted ---------------------------------------------------------------------------------------


test_that("right_names are omitted from the output", {
  left = SubsetOp(arrL, selected_fields = 'laa')

  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa),
      project(R, raa),
      'left_names=laa', 'right_names=raa'
    ),
    laa
  )
  ")

  right = arrR
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa),
      R,
      'left_names=laa', 'right_names=raa'
    ),
    laa
  )
  ")
})

test_that("No selected fields on either side", {
  assert_afl_equal(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa')$to_afl(),
    "equi_join(L, R, 'left_names=laa', 'right_names=raa')"
  )

  left = SubsetOp(arrL, filter_expr = e(lab > 1, lac == 'val'))
  assert_afl_equal(
    JoinOp(left, arrR, on_left = 'laa', on_right = 'raa')$to_afl(),
    "equi_join(filter(L, lab > 1 and lac = 'val'), R, 'left_names=laa', 'right_names=raa')"
  )
})


# Join on array dimensions ----------------------------------------------------------------------------------------


test_that("Join on dimensions and select other dimensions", {
  left = SubsetOp(arrL, selected_fields = 'lda', artificial_attr = 'dummy1')
  right = SubsetOp(arrR, selected_fields = c('rda'), artificial_attr = 'dummy2')
  joinOp = JoinOp(left, right, on_left = 'ldb', on_right = 'rdb')

  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, lda, lda), lda),
      project(apply(R, rda, rda), rda),
      'left_names=ldb', 'right_names=rdb'
    ),
    lda, rda
  )
  ")
})

test_that("Join on dimensions and select other dimensions", {
  left = SubsetOp(arrL, selected_fields = c('laa'))
  right = SubsetOp(arrR, selected_fields = c('rdb'))
  joinOp = JoinOp(left, right, on_left = 'lda', on_right = 'rda')

  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa),
    project(apply(R, rdb, rdb), rdb),
      'left_names=lda', 'right_names=rda'
  ), laa, rdb)
  ")
})


# Customized join settings ----------------------------------------------------------------------------------------

test_that("Add extra settings (equi_join specific)", {
  left = SubsetOp(arrL, selected_fields = c('laa'))
  right = SubsetOp(arrR, selected_fields = c('raa'))
  joinOp = JoinOp(left, right, on_left = 'laa', on_right = 'raa',
    settings = list(algorithm = 'hash_replicate_right', left_outer = 1))

  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa),
    project(R, raa),
      'left_names=laa', 'right_names=raa',
      'algorithm=hash_replicate_right', 'left_outer=1'
  ), laa)
  ")
})

# Assert JoinOp fields --------------------------------------------------------------------------------------------

#
# When keep_dimension is not speicifed or set to 0
#
test_that("No selcted_fields on either operands. All attrs are selected", {
  joinOp = JoinOp(arrL, arrR, on_left = 'lab', on_right = 'raa')
  expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_names(.SEL), as.character(c()))
  expect_identical(joinOp$get_field_names(.ATR), c('lab', 'laa', 'lac', 'lad', 'rab', 'rac', 'rad'))
  expect_identical(joinOp$get_field_names(.OWN), c('instance_id', 'value_no',
    c('lab', 'laa', 'lac', 'lad', 'rab', 'rac', 'rad')))

  for(joinOp in list(
    JoinOp(arrL, arrR, on_left = 'lda', on_right = 'raa'),
    JoinOp(arrL, arrR, on_left = 'lda', on_right = 'raa', settings = list(keep_dimensions=0)),
    JoinOp(arrL, SubsetOp(arrR, filter_expr=e(rab > 0)), on_left = 'lda', on_right = 'raa', settings = list(keep_dimensions=0)),
    JoinOp(SubsetOp(arrL, filter_expr=e(ldc < 0)), SubsetOp(arrR, filter_expr=e(rab > 0)), on_left = 'lda', on_right = 'raa')
  )){
    expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
    expect_identical(joinOp$get_field_names(.SEL), as.character(c()))
    expect_identical(joinOp$get_field_names(.ATR), c('lda', 'laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad'))
    expect_identical(joinOp$get_field_names(.OWN), c('instance_id', 'value_no',
      c('lda', 'laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad')))
  }
})

test_that("When left has selected_fields, JoinOp will project on equi_join which changes fields", {
  leftSelected = list(c('laa'), c('laa', 'lab'), c('lda', 'ldb'), c('lda', 'laa'))
  for(lSelected in leftSelected){
    left = SubsetOp(arrL, selected_fields = lSelected)
    joinOp = JoinOp(left, arrR, on_left = 'ldc', on_right = 'raa')
    expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
    expect_identical(joinOp$get_field_names(.SEL), lSelected)
    expect_identical(joinOp$get_field_names(.ATR), lSelected)
    expect_identical(joinOp$get_field_names(.OWN), c('instance_id', 'value_no', lSelected))
  }
})

test_that("When right has selected_fields, the 'on_right' keys are excluded", {
  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(arrL, right, on_left = 'lda', on_right = 'rda')
  expect_identical(joinOp$get_field_names(.SEL), c('raa'))
  expect_identical(joinOp$get_field_names(.ATR), c('raa'))

  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(arrL, right, on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$get_field_names(.SEL), as.character(c()))
  expect_identical(joinOp$get_field_names(.ATR), as.character(c()))

  left = SubsetOp(arrL, selected_fields = 'laa')
  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(left, right, on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$get_field_names(.SEL), 'laa')
  expect_identical(joinOp$get_field_names(.ATR), 'laa')
})


#
# When keep_dimension is set to 1, left/right dimensions are automatically included in equi_join
#
test_that("No selcted_fields on either operands. Left join keys and left/right dimensions are selected", {
  joinOp = JoinOp(arrL, arrR, on_left = 'lab', on_right = 'raa', settings=list(keep_dimensions=1))
  assert_afl_equal(joinOp$to_afl(), "equi_join(
    L, R, 'left_names=lab', 'right_names=raa', 'keep_dimensions=1'
  )")
  expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_names(.SEL), as.character(c()))
  expect_identical(joinOp$get_field_names(.ATR),
    c('lab', 'laa', 'lac', 'lad', arrL$get_field_names(.DIM), 'rab', 'rac', 'rad', arrR$get_field_names(.DIM)))
  expect_identical(joinOp$get_field_names(.OWN), c('instance_id', 'value_no',
    joinOp$get_field_names(.ATR)))
})

# In below two tests, even we speicify 'keep_dimensions=1', the result JoinOp will only retain the selected fields
# from both operands by 'project'ing on 'equi_join(...)'.
# The kept dimensions, if not selected, will still be omitted because equi_join convert dimensions to attributes
#
test_that("When left has selected_fields, JoinOp will project on equi_join which changes fields", {
  leftSelected = list(c('laa'), c('laa', 'lab'), c('lda', 'ldb'), c('lda', 'laa'))
  afls = list(
    "project(equi_join(project(L, laa), R, 'left_names=ldc', 'right_names=raa', 'keep_dimensions=1'), laa)"
    , "project(equi_join(project(L, laa, lab), R, 'left_names=ldc', 'right_names=raa', 'keep_dimensions=1'), laa, lab)"
    # TODO: optimize joinOp to_afl
    # , "project(equi_join(L, R, 'left_names=ldc', 'right_names=raa', 'keep_dimensions=1'), lda, ldb)"
    # , "project(equi_join(project(L, laa), R, 'left_names=ldc', 'right_names=raa', 'keep_dimensions=1'), lda, laa)"
  )
  i = 1
  for(lSelected in leftSelected){
    left = SubsetOp(arrL, selected_fields = lSelected)
    joinOp = JoinOp(left, arrR, on_left = 'ldc', on_right = 'raa', settings=list(keep_dimensions=1))
    expect_identical(joinOp$get_field_names(.DIM), c('instance_id', 'value_no'))
    expect_identical(joinOp$get_field_names(.SEL), lSelected)
    expect_identical(joinOp$get_field_names(.ATR), lSelected)
    expect_identical(joinOp$get_field_names(.OWN), c('instance_id', 'value_no', lSelected))
    if(i <= length(afls))
      assert_afl_equal(joinOp$to_afl(), afls[[i]])
    i = i + 1
  }
})

test_that("When right has selected_fields, the 'on_right' keys are excluded", {
  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(arrL, right, on_left = 'lda', on_right = 'rda', settings=list(keep_dimensions=1))
  expect_identical(joinOp$get_field_names(.SEL), c('raa'))
  expect_identical(joinOp$get_field_names(.ATR), c('raa'))
  assert_afl_equal(joinOp$to_afl(),
    "project(
      equi_join(L, project(R, raa), 'left_names=lda', 'right_names=rda', 'keep_dimensions=1'),
      raa
    )")

  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(arrL, right, on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$get_field_names(.SEL), as.character(c()))
  expect_identical(joinOp$get_field_names(.ATR), as.character(c()))

  left = SubsetOp(arrL, selected_fields = 'laa')
  right = SubsetOp(arrR, selected_fields = 'raa')
  joinOp = JoinOp(left, right, on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$get_field_names(.SEL), 'laa')
  expect_identical(joinOp$get_field_names(.ATR), 'laa')
})


# Select fields on JoinOp  ----------------------------------------------------------------------------------------

test_that("Use JoinOp in a SubsetOp to select fields from equi_join", {
  joinOp = JoinOp(arrL, arrR, on_left = 'laa', on_right = 'rda')
  for(selected in list(
    c('lab', 'rab'),
    c('laa', 'rab', 'rac', 'lac')
  )){
    subsetOp = SubsetOp(joinOp, selected_fields = selected)
    expect_identical(subsetOp$get_field_names(.SEL), selected)
  }

  # Since dimensions are not included by default, errors below are expected
  joinOp = JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa')
  for(selected in list(
    c('lda', 'rab'),  # No 'lda' field
    c('laa', 'rab', 'rda', 'lac')  # No 'rda' field
  )){
    expect_error(SubsetOp(joinOp, selected_fields = selected))
  }
})

test_that("Set keep_dimensions=1 to select all fields expect those in on_right", {
  expect_false(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa'),
    selected_fields = c('laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad')
  )))
  expect_false(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'lda', on_right = 'raa'),
    selected_fields = c('lda', 'lab', 'rac', 'rad')
  )))

  # Without keep_dimensions=1, no dimensions are included
  expect_error(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa'),
    selected_fields = c('lda')
  )))
  expect_error(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa'),
    selected_fields = c('rda')
  )))
  expect_error(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa', settings=list(keep_dimensions=0)),
    selected_fields = c('rda')
  )))
  expect_false(is.null(SubsetOp(
    JoinOp(arrL, arrR, on_left = 'laa', on_right = 'raa', settings=list(keep_dimensions=1)),
    selected_fields = c('lda', 'rda', 'rdd')
  )))
})
