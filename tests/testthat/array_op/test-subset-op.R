context("Test SubsetOp")

operand = AnyArrayOp('operand', c('da', 'db', 'dc', 'dd'), c('aa', 'ab', 'ac', 'ad'))


# Cannot select non-existent fields -------------------------------------------------------------------------------

test_that("selected_fields must be valid dimensions or attributes of the operand", {
  expect_error(SubsetOp(operand, selected_fields = c('non')), 'non')
  expect_error(SubsetOp(operand, selected_fields = c('aa', 'non')), 'non')
})

# Fields assertion ------------------------------------------------------------------------------------------------
# A SubsetOp ONLY changes the source operand fields when there is non-empty 'selected_fields' specified
# Otherwise, SubsetOp has the identical sets of fields (ie. dimensions, attributes and owned fields)

test_that("Without filter or selected fields", {

  for(subsetOp in list(
    SubsetOp(operand),
    SubsetOp(operand, filter_expr = NULL),
    SubsetOp(operand, filter_expr = NULL, selected_fields = as.character(c()))
  )){
    owned = c('da', 'db', 'dc', 'dd', 'aa', 'ab', 'ac', 'ad')
    expect_identical(subsetOp$get_field_names(.OWN), owned)
    expect_identical(subsetOp$get_field_types(owned), list(
      da='dt_da', db='dt_db', dc='dt_dc', dd='dt_dd', aa='dt_aa', ab='dt_ab', ac='dt_ac', ad='dt_ad')
    )
  }
})

test_that("Filter does not change fields", {
  for(subsetOp in list(
    SubsetOp(operand, filter_expr = e(aa > 1)),
    SubsetOp(operand, filter_expr = e(da > 1)),
    SubsetOp(operand, filter_expr = e(da > 1, ad < 42))
  )){
    owned = c('da', 'db', 'dc', 'dd', 'aa', 'ab', 'ac', 'ad')
    expect_identical(subsetOp$get_field_names(.OWN), owned)
    expect_identical(subsetOp$get_field_names(.DIM), c('da', 'db', 'dc', 'dd'))
    expect_identical(subsetOp$get_field_names(.ATR), c('aa', 'ab', 'ac', 'ad'))
    expect_identical(subsetOp$get_field_types(owned), list(
      da='dt_da', db='dt_db', dc='dt_dc', dd='dt_dd', aa='dt_aa', ab='dt_ab', ac='dt_ac', ad='dt_ad')
    )
  }
})

test_that("selected_fields reduces SubsetOp attrs, but NOT dimensions", {
  for(subsetOp in list(
    SubsetOp(operand, selected_fields = 'aa'),
    SubsetOp(operand, selected_fields = c('da', 'aa'))
  )){
    expect_identical(subsetOp$get_field_names(.OWN), c('da', 'db', 'dc', 'dd', 'aa'))
    expect_identical(subsetOp$get_field_names(.DIM), c('da', 'db', 'dc', 'dd'))
    expect_identical(subsetOp$get_field_names(.ATR), c('aa'))
    expect_identical(subsetOp$get_field_types('aa'), list(aa='dt_aa'))
    expect_identical(subsetOp$get_field_types('da'), list(da='dt_da'))
  }

  # If selected_fields does not have attrs, then an artificial attr is generated (ie. apply'ed).
  # But it should not concern the client code
  for(subsetOp in list(
    SubsetOp(operand, selected_fields = 'da'),
    SubsetOp(operand, selected_fields = c('da', 'db'))
  )){
    expect_identical(subsetOp$get_field_names(.OWN), c('da', 'db', 'dc', 'dd'))
    expect_identical(subsetOp$get_field_names(.DIM), c('da', 'db', 'dc', 'dd'))
    # The artifical field is assigned a random name and should not concern ArrayOp users.
    expect_identical(subsetOp$get_field_names(.ATR), as.character(c()))
    expect_identical(subsetOp$get_field_types(
      subsetOp$get_field_names(.ATR)), EMPTY_NAMED_LIST)
  }
})


# to_afl() --------------------------------------------------------------------------------------------------------

test_that("Return the operand$to_afl() if no filter or selected fields", {
  for(subsetOp in list(
    SubsetOp(operand),
    SubsetOp(operand, filter_expr = NULL),
    SubsetOp(operand, filter_expr = NULL, selected_fields = as.character(c()))
  )){
    assert_afl_equal(subsetOp$to_afl(), "operand")
  }
})

test_that("Selected operand attributes are 'project'ed ", {
  assert_afl_equal(SubsetOp(operand, selected_fields = c('aa'))$to_afl(), 'project(operand, aa)')
  assert_afl_equal(SubsetOp(operand, selected_fields = c('aa', 'ad'))$to_afl(), 'project(operand, aa, ad)')
  assert_afl_equal(SubsetOp(operand, selected_fields = c('ac', 'aa', 'ad'))$to_afl(), 'project(operand, ac, aa, ad)')
})

test_that("Selected operand dimensions are ignored by default", {
  assert_afl_equal(SubsetOp(operand, selected_fields = c('da', 'aa'))$to_afl(), 'project(operand, aa)')
  assert_afl_equal(SubsetOp(operand, selected_fields = c('ac', 'da', 'aa'))$to_afl(), 'project(operand, ac, aa)')
})

test_that("SubsetOp can be an operand of any other ArrayOps", {
  subNested = SubsetOp(operand, selected_fields = c('aa', 'ab', 'ac'))
  sub = SubsetOp(subNested, selected_fields = c('ac'))
  # It may not make sense to SubsetOp multiple times on an array.
  # E.g. project(operand, ac) will be semantically equivelent to this example.
  # This is for demonstration only.
  assert_afl_equal(sub$to_afl(), 'project(project(operand, aa, ab, ac), ac)')

  subNested = SubsetOp(operand, selected_fields = as.character(c()))
  sub = SubsetOp(subNested, selected_fields = c('ac'))
  assert_afl_equal(sub$to_afl(), 'project(operand, ac)')
  assert_afl_equal(SubsetOp(sub)$to_afl(), 'project(operand, ac)')
  assert_afl_equal(SubsetOp(sub, selected_fields = c('ac'))$to_afl(), 'project(project(operand, ac), ac)')
})

test_that("Filter only, no selected_fields", {
  # e(...) return a list of R call expressions
  assert_afl_equal(SubsetOp(operand, filter_expr = e(aa == 42))$to_afl(), 'filter(operand, aa = 42)')
  assert_afl_equal(SubsetOp(operand, filter_expr = e(aa == 42)[[1]])$to_afl(), 'filter(operand, aa = 42)')
  # Explicitly 'merge' ExprsList into a single Expression
  assert_afl_equal(SubsetOp(operand, filter_expr = e_merge(e(aa == 42, da > 1)))$to_afl(), 'filter(operand, aa = 42 and da > 1)')
  # Or SubsetOp initialization function will do that
  assert_afl_equal(SubsetOp(operand, filter_expr = e(aa == 42, da > 1))$to_afl(), 'filter(operand, aa = 42 and da > 1)')
  # 'e_merge' is more verbose but we can specify the mode for 'merge'
  assert_afl_equal(SubsetOp(operand, filter_expr = e_merge(e(aa == 42, da > 1), mode = 'OR'))$to_afl(),
    'filter(operand, aa = 42 or da > 1)')
})

test_that("Filter and Selected fields", {
  assert_afl_equal(SubsetOp(operand, filter_expr = e(aa == 42), selected_fields = c('aa'))$to_afl(),
    'project(filter(operand, aa = 42), aa)')
  assert_afl_equal(SubsetOp(operand, filter_expr = e(aa == 42), selected_fields = c('ab'))$to_afl(),
    'project(filter(operand, aa = 42), ab)')
  assert_afl_equal(SubsetOp(operand, filter_expr = e(da == 42), selected_fields = c('ab', 'ad'))$to_afl(),
    'project(filter(operand, da = 42), ab, ad)')
})

test_that("When no attrs selected and dimensions are selected, an artifical attr is created and then projected", {
  # artificial_attr is specified for testing purpose only. If omitted, a random string is generated to avoid name clashes
  sub = SubsetOp(operand, selected_fields = c('da', 'db'), artificial_attr = 'dummy')
  assert_afl_equal(sub$to_afl(), "project(apply(operand, dummy, null), dummy)")

  # If no selected_fields provided, then there is no intent to reduce fields at all.
  assert_afl_equal(
    SubsetOp(operand, selected_fields = c(), artificial_attr = 'dummy')$to_afl(),
    "operand"
  )

  sub = SubsetOp(operand, selected_fields = c('da', 'db'))
  assert_afl_equal(sub$to_afl(),
    sprintf("project(apply(operand, %s, null), %s)", sub$artificial_attr, sub$artificial_attr))

  sub = SubsetOp(operand, filter_expr = e(aa > 1, ab == 'value'), selected_fields = c('da', 'db'))
  assert_afl_equal(sub$to_afl(),
    sprintf("project(apply(filter(operand, aa > 1 and ab = 'value'), %s, null), %s)", sub$artificial_attr, sub$artificial_attr))
})


test_that("to_afl(drop_dims=T) selected dims are applied and then projected", {
  # No difference if only attributes are selected
  assert_afl_equal(SubsetOp(operand, selected_fields = c('aa'))$to_afl(), "project(operand, aa)")
  assert_afl_equal(SubsetOp(operand, selected_fields = c('aa'))$to_afl(drop_dims=T), "project(operand, aa)")

  # Different if at least one dimension is selected
  assert_afl_equal(
    SubsetOp(operand, selected_fields = c('da'), artificial_attr='dummy')$to_afl(),
    "project(apply(operand, dummy, null), dummy)")
  assert_afl_equal(SubsetOp(operand, selected_fields = c('da'))$to_afl(drop_dims=T),
    "project(apply(operand, da, da), da)")
  assert_afl_equal(SubsetOp(operand, selected_fields = c('aa', 'db'))$to_afl(drop_dims=T),
    "project(apply(operand, db, db), aa, db)")
})


# to_join_operand_afl  ----------------------------------------------------------------------------------------------------
# Detailed design and logic flow can be found at:
# https://docs.google.com/spreadsheets/d/1kN7QgvQXXxcovW9q25d4TNb6tsf888-xhWdZW-WELWw/edit?usp=sharing
# Here the schema 'x' has fields defined pertaining to specs at above link.
# x schema: <aa, ab, ac, ad>[da, db, dc, dd]  field types are ignored intentionally
# "project(
#            equi_join(
#                           project(apply(x, ...), ...),
#                            y ),
#            ...)"


# Scidb 'project' operator:
# 1. retains dimensions.
# 2. cannot 'project' dimensions
# 3. removes non-projected attributes
#
# Scidb 'apply' operator:
# 1. converts a dimension to an attribute
#
# Note: the outer project is NOT tested here.

x = AnyArrayOp('x', c('da', 'db', 'dc', 'dd'), c('aa', 'ab', 'ac', 'ad'))

test_that("to_join_operand_afl, case: da", {
  op = SubsetOp(x, selected_fields = c('da', 'aa'))
  assert_afl_equal(op$to_join_operand_afl('da'), "project(x, aa)")

  # Since we cannot 'project' dimensions and have no attributes to project,
  # We have to 'apply' an artificial attribute
  op = SubsetOp(x, selected_fields =  c('da'), artificial_attr = 'dummy')
  assert_afl_equal(op$to_join_operand_afl('da'),
    "project(apply(x, dummy, null), dummy)")
  # One artificial attribute is sufficient even for multiple join fields
  op = SubsetOp(x, selected_fields =  c('da', 'db'), artificial_attr = 'dummy')
  # Here we treat dimension 'db' as 'da's role (ie. required and selected)
  assert_afl_equal(op$to_join_operand_afl(c('da', 'db')),
    "project(apply(x, dummy, null), dummy)")
})

test_that("to_join_operand_afl, case: db", {
  # Required but not joined-on dimension: db
  op = SubsetOp(x, selected_fields =  c('db'))
  assert_afl_equal(op$to_join_operand_afl('da'), "project(apply(x, db, db), db)")
})

test_that("to_join_operand_afl, case: dc", {
  # Joined-on but not required dimension: dc

  # an attribute is selected
  op = SubsetOp(x, selected_fields =  c('aa'))
  assert_afl_equal(op$to_join_operand_afl('dc'), "project(x, aa)")

  # an dimension is selected
  op = SubsetOp(x, selected_fields =  c('db'))
  assert_afl_equal(op$to_join_operand_afl('dc'), "project(apply(x, db, db), db)")

  # Combined with filter
  op = SubsetOp(x, selected_fields =  c('db'), filter_expr = e(da > 1))
  assert_afl_equal(op$to_join_operand_afl('dc'), "project(apply(filter(x, da > 1), db, db), db)")
})

test_that("to_join_operand_afl, case: aa", {
  # Required and joined-on attribute: aa
  op = SubsetOp(x, selected_fields =  c('aa'))
  assert_afl_equal(op$to_join_operand_afl('aa'), "project(x, aa)")
})

test_that("to_join_operand_afl, case: ab", {
  # Required but not joined-on attribute: ab
  op = SubsetOp(x, selected_fields =  c('ab'))
  assert_afl_equal(op$to_join_operand_afl('da'), "project(x, ab)")
})

test_that("to_join_operand_afl, case: ac", {
  # Joined-on but not required attribute: ac
  op = SubsetOp(x, selected_fields =  c('ab'))
  # We don't need to project 'ab' since any joined-on field (attribute/dimension) will be retained as
  # an attribute in the resulant equi_join
  assert_afl_equal(op$to_join_operand_afl('ac'), "project(x, ab)")
})

# If no selected_fields, filter only, then we do not 'project'

test_that("No selected_fields, filter only", {
  assert_afl_equal(
    SubsetOp(x, filter_expr = e(da > 1, aa == 'val'))$to_join_operand_afl('ab'),
    "filter(x, da>1 and aa='val')"
  )
  assert_afl_equal(
    SubsetOp(x, filter_expr = e(da > 1, aa == 'val'))$to_join_operand_afl('da'),
    "filter(x, da>1 and aa='val')"
  )
  assert_afl_equal(
    SubsetOp(x, filter_expr = e(da > 1, aa == 'val'))$to_join_operand_afl('aa'),
    "filter(x, da>1 and aa='val')"
  )
  assert_afl_equal(
    SubsetOp(x, filter_expr = e(da > 1, aa == 'val'))$to_join_operand_afl('db'),
    "filter(x, da>1 and aa='val')"
  )
})


# Select field aliases --------------------------------------------------------------------------------------------

# test_that("Select new field name derived from existing ones", {
#   s = AnyArrayOp('operand', c('da', 'db'), c('aa', 'ab'))
#   subsetOp = SubsetOp(s, selected_fields = c('da->da_alias'))
#   expect_identical()
# })
