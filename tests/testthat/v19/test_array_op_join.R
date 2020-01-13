context("Scidb V19.x ArrayOp$join")

# Join ------------------------------------------------------------------------------------------------------------

# Join operands. Acronyms are left/right-dimension/attr-filedName.
# E.g. lda: left dimension 'a', rac: right attribute 'c'
# Note that the 'on_right' keys will be omitted from join results per equi_join V18 implementation.

dummyArrayOp = function(name, dims, attrs) {
  fields = c(dims, attrs)
  dtypes = structure(lapply(fields, function(x) sprintf("dt_%s", x)), names = fields)
  newArrayOp(name, dims, attrs, dtypes = dtypes)
}

arrL =  dummyArrayOp('L', c('lda', 'ldb', 'ldc', 'ldd'), c('laa', 'lab', 'lac', 'lad'))
arrR = dummyArrayOp('R', c('rda', 'rdb', 'rdc', 'rdd'), c('raa', 'rab', 'rac', 'rad'))


# Join operand alias ----------------------------------------------------------------------------------------------

test_that("Operand alias", {
  joinOp = arrL$select('laa')$join(arrR, on_left = 'laa', on_right = 'raa', .left_alias = 'LeftL', .right_alias = 'RightR')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as LeftL,
      R as RightR,
        left_names:LeftL.laa, right_names:RightR.raa
    ),
    laa
  )")
})

# __Join on array attributes -------------------------------------------------------------------------------------

# NOTE: JoinOp ignores all 'on_right' fields even they are 'selected' in 'right' operand
# because equi_join only keeps the 'on_left' fields
#
test_that("Join on selected attr", {
  
  joinOp = arrL$select('laa')$join(arrR, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      R as _R,
        left_names:_L.laa, right_names:_R.raa
    ),
    laa
  )")
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, 'laa')
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_types(c('laa')), list(laa = 'dt_laa'))
  
  # Right operand's selected field can be masked by its join keys 'on_right'.
  joinOp = arrL$select('laa')$join(arrR$select('raa'), on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      project(R, raa) as _R,
        left_names:_L.laa, right_names:_R.raa
    ), 
    laa
  )")
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, 'laa')
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_types(c('laa')), list(laa = 'dt_laa'))
})

test_that("Join on single attribute with .dim_mode = 'drop'", {
  
  joinOp = arrL$select('laa')$join(arrR, on_left = 'laa', on_right = 'raa', 
    .dim_mode = 'drop', .artificial_field = 'z')
  assert_afl_equal(joinOp$to_afl(), "
  project(unpack(
    equi_join(
      project(L, laa) as _L,
      R as _R,
        left_names:_L.laa, right_names:_R.raa
    ),
    z),
    laa
  )")
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, 'laa')
  expect_identical(joinOp$dims, c('z'))
  expect_identical(joinOp$get_field_types(c('laa')), list(laa = 'dt_laa'))
})

test_that("Join on multiple attrs", {
  left = arrL$select('laa', 'lab')
  right = arrR$select('raa', 'rac')
  joinOp = left$join(right, 
    on_left = c('laa', 'lab'), on_right = c('raa', 'rac'))
  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa, lab) as _L,
    project(R, raa, rac) as _R,
      left_names:(_L.laa,_L.lab), right_names:(_R.raa,_R.rac)
  ), laa, lab)
  ")
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, c('laa', 'lab'))
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$get_field_types(c('laa', 'lab')), list(laa = 'dt_laa', lab = 'dt_lab'))
  
  joinOp = left$join(right, 
    on_left = c('laa', 'lab'), on_right = c('raa', 'rac'), 
    .dim_mode = 'drop', .artificial_field = 'z')

  assert_afl_equal(joinOp$to_afl(), "project(unpack(
  equi_join(
    project(L, laa, lab) as _L,
    project(R, raa, rac) as _R,
      left_names:(_L.laa,_L.lab), right_names:(_R.raa,_R.rac)
  ), z), laa, lab)
  ")
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, c('laa', 'lab'))
  expect_identical(joinOp$dims, 'z')
  expect_identical(joinOp$get_field_types(c('laa', 'lab')), list(laa = 'dt_laa', lab = 'dt_lab'))
})

test_that("No selected fields in right operand", {
  joinOp = arrL$select('laa')$join(arrR, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      R as _R,
      left_names:_L.laa, right_names:_R.raa
    ),
    laa
  )")

  # Select L attr and join on L dimension
  joinOp = arrL$select('laa')$join(arrR, on_left = 'lda', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      R as _R,
      left_names:_L.lda, right_names:_R.raa
    ),
    laa
  )")

  # Select L dimension and join on L attribute
  joinOp = arrL$select('lda', 'laa')$join(arrR, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project( apply(L, lda, lda), lda, laa) as _L,
      R as _R,
      left_names:_L.laa, right_names:_R.raa
    ),
    lda, laa
  )")
})

test_that("No selected fields on either side", {
  # Default output includes left.join_keys + (left.attrs-join_keys) + (right.attrs-join_keys)
  joinOp = arrL$join(arrR, on_left = 'laa', on_right = 'raa')
  assert_afl_equal(
    joinOp$to_afl(),
    "equi_join(L as _L, R as _R, left_names:_L.laa, right_names:_R.raa)"
  )
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$attrs, c('laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad'))
  expect_equal(length(joinOp$dtypes), 9)
  
  # If keep_dimension = 1, the output includes 
  # left.join_keys + (left.attrs-join_keys+left.dims) + (right.attrs-join_keys+right.dims)
  
  joinOp = arrL$join(arrR, on_left = 'laa', on_right = 'raa', list(keep_dimensions = 1))
  assert_afl_equal(
    joinOp$to_afl(),
    "equi_join(L as _L, R as _R, left_names:_L.laa, right_names:_R.raa, keep_dimensions:1)"
  )
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$attrs, c('laa', 'lab', 'lac', 'lad', 'lda', 'ldb', 'ldc', 'ldd',
    'rab', 'rac', 'rad', 'rda', 'rdb', 'rdc', 'rdd'))
  expect_equal(length(joinOp$dtypes), 17)
  
})

# __on_right keys are omitted  ------------------------------------------------------------------------------------


test_that("Right operand's selected fields are omitted from the output if they are part of on_right join keys", {
  joinOp = arrL$select('laa')$join(arrR$select('raa'), on_left = 'laa', on_right = 'raa')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      project(R, raa) as _R,
      left_names:_L.laa, right_names:_R.raa
    ),
    laa
  )
  ")
})


# __Join on array dimensions ----------------------------------------------------------------------------------------


test_that("Join on dims and select other dims", {
  joinOp = arrL$select('lda')$join(arrR$select('rda'), on_left = 'ldb', on_right = 'rdb')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, lda, lda), lda) as _L,
      project(apply(R, rda, rda), rda) as _R,
      left_names:_L.ldb, right_names:_R.rdb
    ),
    lda, rda
  )")
  expect_identical(joinOp$attrs, c('lda', 'rda'))
})

test_that("Join on dims and select attrs", {
  joinOp = arrL$select('laa')$join(arrR$select('rdb'), on_left = 'lda', on_right = 'rda')

  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      project(apply(R, rdb, rdb), rdb) as _R,
        left_names:_L.lda, right_names:_R.rda
    ), laa, rdb
  )")
  expect_identical(joinOp$attrs, c('laa', 'rdb'))
})

test_that("Special case when operand's selected fields are all join keys", {
  joinOp = arrL$select('lda')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, x, null), x) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa
    ), lda
  )
  ")
  joinOp = arrL$select('lda', 'ldb')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, ldb, ldb), ldb) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa
    ), lda, ldb
  )")
  joinOp = arrL$select('lda', 'laa')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x')
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa
    ), lda, laa
  )
  ")
  
  # Keep_dimensions = 1
  joinOp = arrL$select('lda')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x', 
    setting = list(keep_dimensions = 1))
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, x, null), x) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa, keep_dimensions:1
    ), lda
  )
  ")
  joinOp = arrL$select('lda', 'ldb')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x', 
    setting = list(keep_dimensions = 1))
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(apply(L, x, null), x) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa, keep_dimensions:1
    ), lda, ldb
  )
  ")
  joinOp = arrL$select('lda', 'laa')$join(arrR, on_left = 'lda', on_right = 'raa', .artificial_field = 'x', 
    setting = list(keep_dimensions = 1))
  assert_afl_equal(joinOp$to_afl(), "
  project(
    equi_join(
      project(L, laa) as _L,
      R as _R,
        left_names:_L.lda, right_names:_R.raa, keep_dimensions:1
    ), lda, laa
  )
  ")
})


# __Customized join settings ----------------------------------------------------------------------------------------

test_that("Add extra settings (equi_join specific)", {
  joinOp = arrL$select('laa')$join(arrR$select('raa'), on_left = 'laa', on_right = 'raa',
    settings = list(algorithm = 'hash_replicate_right', left_outer = 1))

  assert_afl_equal(joinOp$to_afl(), "project(
  equi_join(
    project(L, laa) as _L,
    project(R, raa) as _R,
      left_names:_L.laa, right_names:_R.raa,
      algorithm:hash_replicate_right, left_outer:1
  ), laa)
  ")
})

# __Assert JoinOp fields --------------------------------------------------------------------------------------------

#
# When keep_dimension is not speicifed or set to 0
#
test_that("No selcted_fields on either operands. All attrs are selected", {
  joinOp = arrL$join(arrR, on_left = 'lab', on_right = 'raa')
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, c('lab', 'laa', 'lac', 'lad', 'rab', 'rac', 'rad'))
  expect_identical(joinOp$dims_n_attrs, c('instance_id', 'value_no',
    c('lab', 'laa', 'lac', 'lad', 'rab', 'rac', 'rad')))

  for(joinOp in list(
    arrL$join(arrR, on_left = 'lda', on_right = 'raa'),
    arrL$join(arrR, on_left = 'lda', on_right = 'raa', settings = list(keep_dimensions=0)),
    arrL$join(arrR$where(rab > 0), on_left = 'lda', on_right = 'raa', settings = list(keep_dimensions=0)),
    arrL$where(ldc < 0)$join(arrR$where(rab > 0), on_left = 'lda', on_right = 'raa')
  )){
    expect_identical(joinOp$dims, c('instance_id', 'value_no'))
    expect_identical(joinOp$selected, NULL)
    expect_identical(joinOp$attrs, c('lda', 'laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad'))
    expect_identical(joinOp$dims_n_attrs, c('instance_id', 'value_no',
      c('lda', 'laa', 'lab', 'lac', 'lad', 'rab', 'rac', 'rad')))
  }
})
test_that("When left has selected_fields, JoinOp will project on equi_join which changes fields", {
  leftSelected = list(c('laa'), c('laa', 'lab'), c('lda', 'ldb'), c('lda', 'laa'))
  for(lSelected in leftSelected){
    left = do.call(arrL$select, as.list(lSelected))
    joinOp = left$join(arrR, on_left = 'ldc', on_right = 'raa')
    expect_identical(joinOp$dims, c('instance_id', 'value_no'))
    expect_identical(joinOp$selected, NULL)
    expect_identical(joinOp$attrs, lSelected)
    expect_identical(joinOp$dims_n_attrs, c('instance_id', 'value_no', lSelected))
  }
})

test_that("When right has selected_fields, the 'on_right' keys are excluded", {
  joinOp = arrL$join(arrR$select('raa'), on_left = 'lda', on_right = 'rda')
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, c('raa'))
  
  # Cannot select on right fields only when they are also join keys
  expect_error(arrL$join(arrR$select('raa'), on_left = 'lda', on_right = 'raa'), "Right operand")

  joinOp = arrL$select('laa')$join(arrR$select('raa'), on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs, 'laa')
})


#
# When keep_dimension is set to 1, left/right dimensions are automatically included in result
#
test_that("No selcted_fields on either operands. Left join keys and left/right dimensions are selected", {
  joinOp = arrL$join(arrR, on_left = 'lab', on_right = 'raa', settings=list(keep_dimensions=1))
  assert_afl_equal(joinOp$to_afl(), "equi_join(
    L as _L, R as _R, left_names:_L.lab, right_names:_R.raa, keep_dimensions:1
  )")
  expect_identical(joinOp$dims, c('instance_id', 'value_no'))
  expect_identical(joinOp$selected, NULL)
  expect_identical(joinOp$attrs,
    c('lab', 'laa', 'lac', 'lad', arrL$dims, 'rab', 'rac', 'rad', arrR$dims))
  expect_identical(joinOp$dims_n_attrs, c('instance_id', 'value_no',
    joinOp$attrs))
})

# In below two tests, even we speicify keep_dimensions:1, the result JoinOp will only retain the selected fields
# from both operands (if provided) by 'project'ing on 'equi_join(...)'.
# The kept dimensions, if not selected, will still be omitted because equi_join convert dimensions to attributes
#
test_that("When left has selected_fields, JoinOp will project on equi_join which changes fields", {
  leftSelected = list(c('laa'), c('laa', 'lab'), c('lda', 'ldb'), c('lda', 'laa'))
  afls = list(
    "project(equi_join(project(L, laa) as _L, R as _R, left_names:_L.ldc, right_names:_R.raa, keep_dimensions:1), laa)"
    , "project(equi_join(project(L, laa, lab) as _L, R as _R, left_names:_L.ldc, right_names:_R.raa, keep_dimensions:1), laa, lab)"
    , "project(equi_join(project(apply(L, xxx, null), xxx) as _L, R as _R, left_names:_L.ldc, right_names:_R.raa, keep_dimensions:1), lda, ldb)"
    , "project(equi_join(project(L, laa) as _L, R as _R, left_names:_L.ldc, right_names:_R.raa, keep_dimensions:1), lda, laa)"
  )
  i = 1
  for(lSelected in leftSelected){
    left = do.call(arrL$select, as.list(lSelected))
    joinOp = left$join(arrR, on_left = 'ldc', on_right = 'raa', .artificial_field = 'xxx', settings=list(keep_dimensions=1))
    expect_identical(joinOp$dims, c('instance_id', 'value_no'))
    expect_identical(joinOp$selected, NULL)
    expect_identical(joinOp$attrs, lSelected)
    expect_identical(joinOp$dims_n_attrs, c('instance_id', 'value_no', lSelected))
    assert_afl_equal(joinOp$to_afl(), afls[[i]])
    i = i + 1
  }
})

test_that("When right has selected_fields, the 'on_right' keys are excluded", {
  joinOp = arrL$join(arrR$select('raa'), on_left = 'lda', on_right = 'rda', settings=list(keep_dimensions=1))
  expect_identical(joinOp$attrs, c('raa'))
  assert_afl_equal(joinOp$to_afl(),
    "project(
      equi_join(L as _L, project(R, raa) as _R, left_names:_L.lda, right_names:_R.rda, keep_dimensions:1),
      raa
    )")

  joinOp = arrL$select('laa')$join(arrR$select('raa'), on_left = 'lda', on_right = 'raa')
  expect_identical(joinOp$attrs, 'laa')
})

