context("Test newArrayOp base class")

newArrayOp = function(...) ArrayOpBase$new(...)

# Scidb array basics ----------------------------------------------------------------------------------------------


test_that("Raw afl, dimensions, attributes work as expected", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"))
  expect_identical(op$to_afl(), "rawafl")
  expect_identical(op$dims, c("da", "db"))
  expect_identical(op$attrs, c("ac", "ad"))
  # Since no dtypes are passed to initilize function, all fields are not annotated with dtypes.
  expect_error(op$get_field_types(), 'not annotated')
})

test_that("Raw afl, dimensions, attributes work as expected with data types", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_identical(op$to_afl(), "rawafl")
  expect_identical(op$dims, c("da", "db"))
  expect_identical(op$attrs, c("ac", "ad"))
  # By default, get_field_types returns dims+attrs data types
  expect_identical(op$get_field_types(), list(da='int64', db='int64', ac='string', ad='int32'))
  expect_identical(op$get_field_types(op$dims_n_attrs), list(da='int64', db='int64', ac='string', ad='int32'))
  expect_identical(op$get_field_types('da'), list(da='int64'))
  expect_identical(op$get_field_types('ac'), list(ac='string'))
  
  expect_identical(str(op), "rawafl <ac:string,ad:int32> [da;db]")
  expect_identical(print(op), "rawafl <ac:string,ad:int32> [da;db]")
})

test_that("to_df_afl calls to_afl_explict internally", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  assert_afl_equal(op$.to_afl_explicit(), "rawafl")
  assert_afl_equal(op$.to_afl_explicit(drop_dims = T), "rawafl")
  
  # With selected fields
  assert_afl_equal(op$select('ac')$to_df_afl(), "project(rawafl, ac)")
  assert_afl_equal(op$select('ac')$to_df_afl(T), "project(rawafl, ac)")
  assert_afl_equal(op$select(c('da', 'ac'))$to_df_afl(), "project(rawafl, ac)")
  assert_afl_equal(op$select(c('da', 'ac'))$to_df_afl(T), "project(apply(rawafl, da, da), da, ac)")
  
  assert_afl_equal(op$select('da')$to_df_afl(artificial_field = 'x'), 
                   "project(apply(rawafl, x, null), x)")
  assert_afl_equal(op$select('da')$to_df_afl(T, artificial_field = 'x'), 
                   "project(apply(rawafl, da, da), da)")
  
  # With selected fields
  assert_afl_equal(op$.to_afl_explicit(selected_fields = 'ac'), "project(rawafl, ac)")
  assert_afl_equal(op$.to_afl_explicit(T, selected_fields = 'ac'), "project(rawafl, ac)")
  assert_afl_equal(op$.to_afl_explicit(selected_fields = c('da', 'ac')), "project(rawafl, ac)")
  assert_afl_equal(op$.to_afl_explicit(T, selected_fields = c('da', 'ac')), "project(apply(rawafl, da, da), da, ac)")
  
  assert_afl_equal(op$.to_afl_explicit(selected_fields = c('da'), artificial_field = 'x'), 
                   "project(apply(rawafl, x, null), x)")
  assert_afl_equal(op$.to_afl_explicit(T, selected_fields = c('da'), artificial_field = 'x'), 
                   "project(apply(rawafl, da, da), da)")

})

test_that("Differentiate actual data types and raw types", {
  dtypeList = list(da="int64", aa="string", ab="string compression 'zlib'", ac="bool not null")
  rawDtypes = list(da="int64", aa="string", ab="string", ac="bool")
  op = newArrayOp("rawafl", "da", c("aa", "ab", "ac"), 
                  dtypes = dtypeList)
  expect_identical(op$get_field_types(c('da','aa','ab','ac')), dtypeList)
  expect_identical(op$get_field_types(c('da','aa','ab','ac'), .raw=T), rawDtypes)
})

# Where ----------------------------------------------------------------------------------------------------------

test_that("Where an Array using filter expressions", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  # Original op won't be affected
  expect_identical(op$to_afl(), 'rawafl')
  # Multiple filter expressions are treated as AND replation
  assert_afl_equal(op$where(da < 0)$to_afl(), "filter(rawafl, da < 0)")
  assert_afl_equal(op$where(da < 0, ad == 42)$to_afl(), "filter(rawafl, da < 0 and ad = 42)")
  # Use special functions AND/OR to compose complex filter expressions
  assert_afl_equal(op$where(OR(da != 0, db == 3))$to_afl(), "filter(rawafl, da <> 0 or db = 3)")
  
  # Error if filter on non-existent fields
  expect_error(op$where(nonExistent == 42), 'not found')
})

test_that("Resultant newArrayOp has the same schema as the original", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), 
    dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  result = op$where(da <0 , ad == 42)
  expect_identical(result$dims, c('da', 'db'))
  expect_identical(result$attrs, c('ac', 'ad'))
  expect_identical(result$get_field_types(), op$get_field_types())
})


# Select ----------------------------------------------------------------------------------------------------------

test_that("Select fields do not change afl output", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  for(result in c(
    op$select('ac'),
    op$select('ad'),
    op$select('da', 'db', 'ac', 'ad'),
    op$select()
  )){
    # The original op will not be changed
    expect_equal(length(op$selected), 0)
    resultSelected = result$selected
    assert_afl_equal(result$to_afl(), "rawafl")
    # Derive another op
    another = result$select('da', 'ad')
    assert_afl_equal(another$to_afl(), 'rawafl')
    expect_identical(another$selected, c('da', 'ad'))
    expect_identical(result$selected, resultSelected)  # result's selected fields are not changed
  }
})

test_that("Select fields through a field param", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_identical(op$select('ac')$selected, op$select(c('ac'))$selected)
  v = c('ac', 'da')
  expect_identical(op$select('ac', 'da')$selected, op$select(v)$selected)
  expect_identical(op$select('db')$selected, op$select(c('db'))$selected)
  v = c('db', 'ad')
  expect_identical(op$select('db', 'ad')$selected, op$select(v)$selected)
})

test_that("Cannot select non-existent fields", {
  op = newArrayOp("rawafl", c("da", "db"), c("ac", "ad"), dtypes = list(ac='string', ad='int32', da='int64', db='int64'))
  expect_error(op$select('non-existent'))
  expect_error(op$select('da', 'non-exis tent'))
})


# Reshape ---------------------------------------------------------------------------------------------------------

Source = newArrayOp("s", c("da", "db"), c("ac", "ad"), dtypes = list(ac='dtac', ad='dtad', da='dtda', db='dtdb'),
                    dim_specs = list(da="0:1:2:3"))

# 
# dim_mode = 'keep' by default
# 
test_that("Select one attr", {
  for(t in c(
    Source$reshape('ac')
    , Source$reshape(list('ac', 'da'))
    , Source$reshape(c('ac', 'da', 'db'))
    , Source$reshape(c('da', 'ac'))
    , Source$reshape(c('ac', 'da', 'db'), dim_mode = 'keep')
  )){
    expect_identical(t$attrs, 'ac')
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types('ac'), list(ac='dtac'))
    assert_afl_equal(t$to_afl(), "project(s, ac)")
    assert_afl_equal(t$to_schema_str(), "<ac:dtac> [da=0:1:2:3;db]")
  }
})
test_that("Select two attrs", {
  for(t in c(
    Source$reshape(c('ad', 'ac'))
    , Source$reshape(list('ad', 'ac', 'da'))
    , Source$reshape(c('ad', 'ac', 'da', 'db'))
    , Source$reshape(c('ad', 'da', 'ac'))
    , Source$reshape(c('ad', 'ac', 'da', 'db'), dim_mode = 'keep')
  )){
    expect_identical(t$attrs, c('ad', 'ac'))
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types(c('ad', 'ac')), list(ad='dtad', ac='dtac'))
    assert_afl_equal(t$to_afl(), "project(s, ad, ac)")
    assert_afl_equal(t$to_schema_str(), "<ad:dtad, ac:dtac> [da=0:1:2:3;db]")
  }
})
test_that("Select dims only", {
  for(t in c(
    Source$reshape(c('da'), artificial_field = 'z')
    , Source$reshape(list('da', 'db'), artificial_field = 'z')
    , Source$reshape(list('db', 'da'), artificial_field = 'z')
    , Source$reshape(c('da', 'db'), dim_mode = 'keep', artificial_field = 'z')
  )){
    expect_identical(t$attrs, 'z')
    expect_identical(t$dims, c('da', 'db'))
    expect_identical(t$get_field_types(c('da', 'db')), list(da='dtda', db='dtdb'))
    assert_afl_equal(t$to_afl(), "project(apply(s, z, null), z)")
    assert_afl_equal(t$to_schema_str(), "<z:void> [da=0:1:2:3;db]")
  }
})

test_that("Select new fields", {
  # Select no existing attrs
  reshaped = Source$reshape(list(new_field1 = '42'), dtypes = list(new_field1 = 'int'))
  expect_identical(reshaped$attrs, 'new_field1')
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'new_field1' = 'int'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(s, new_field1, 42), new_field1)')
  # Select existing attrs without expression
  reshaped = Source$reshape(list(new_field1 = '42', 'ac'), dtypes = list(new_field1 = 'int'))
  expect_identical(reshaped$attrs, c('new_field1', 'ac'))
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'new_field1' = 'int', 'ac'='dtac'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(s, new_field1, 42), new_field1, ac)')
  
  # Selecting new fields without expression
  reshaped = Source$reshape(list(new_field1 = '42', 'extra'), dtypes = list(new_field1 = 'int', extra='string'))
  expect_identical(reshaped$attrs, c('new_field1', 'extra'))
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'new_field1' = 'int', 'extra'='string'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(s, new_field1, 42), new_field1, extra)')
})

test_that("Provide expressions to exsiting fields will effectively replace the existing fields", {
  # Replace existing field only
  reshaped = Source$reshape(list(ac = 'ac+ad'), dtypes = list(ac='string'))
  expect_identical(reshaped$attrs, 'ac')
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'ac'='string'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(project(apply(s, _ac, ac+ad), _ac), ac, _ac), ac)')
  # Replace existing field while selecting existing fields
  reshaped = Source$reshape(list(ac = 'ac+ad', 'ad'), dtypes = list(ac='string'))
  expect_identical(reshaped$attrs, c('ac', 'ad'))
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'ac'='string', 'ad'='dtad'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(project(apply(s, _ac, ac+ad), ad, _ac), ac, _ac), ac, ad)')
  # Replace existing field while selecting existing fields and new fields
  reshaped = Source$reshape(list(ac = 'ac+ad', 'ad', extra=42), dtypes = list(ac='string', extra='int64'))
  expect_identical(reshaped$attrs, c('ac', 'ad', 'extra'))
  expect_identical(reshaped$dims, c('da', 'db'))
  expect_identical(reshaped$get_field_types(), list('da'='dtda', 'db'='dtdb', 'ac'='string', 'ad'='dtad', extra='int64'))
  assert_afl_equal(reshaped$to_afl(), 'project(apply(project(apply(s, _ac, ac+ad), ad, _ac), ac, _ac, extra, 42), ac, ad, extra)')
})

test_that("Must select fields in dim_mode = 'keep'", {
  expect_error(Source$reshape(), 'select')
  expect_error(Source$reshape(dim_mode = 'keep'), 'select')
})
# 
# dim_mode = 'drop' 
# 
test_that("Select existing attributes/dimensions in dim_mode = 'drop'", {
  # Select attrs
  t = Source$reshape('ac', dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, 'ac')
  expect_identical(t$dims_n_attrs, c('z', 'ac'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), ac)")
  assert_afl_equal(t$to_schema_str(), "<ac:dtac> [z]")
  # Select dims
  t = Source$reshape('da', dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, 'da')
  expect_identical(t$dims_n_attrs, c('z', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), da)")
  assert_afl_equal(t$to_schema_str(), "<da:dtda> [z]")
  # Select dims and attrs
  t = Source$reshape(c('ac', 'da'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('ac', 'da'))
  expect_identical(t$dims_n_attrs, c('z', 'ac', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(unpack(s, z), ac, da)")
  assert_afl_equal(t$to_schema_str(), "<ac:dtac,da:dtda> [z]")
  # No selected fields
  t = Source$reshape(dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('da', 'db', 'ac', 'ad'))
  expect_identical(t$dims_n_attrs, c('z', 'da', 'db', 'ac', 'ad'))
  expect_identical(t$get_field_types(), list(z = 'int64', da='dtda', db='dtdb', ac = 'dtac', ad = 'dtad'))
  expect_null(t$get_dim_specs('z')[[1]])
  assert_afl_equal(t$to_afl(), "unpack(s, z)")
  assert_afl_equal(t$to_schema_str(), "<da:dtda,db:dtdb,ac:dtac,ad:dtad> [z]")
})

test_that("Select new fields in dim_mode = 'drop'", {
  # Select new fields only
  t = Source$reshape(list(nfa = 'strlen(ad)', nfb = '42'), 
    dtypes = list(nfa='int32', nfb='int64'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('nfa', 'nfb'))
  expect_identical(t$get_field_types(), list(z = 'int64', nfa = 'int32', nfb = 'int64'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad), nfb, 42), nfa, nfb)")
  
  # Select new fields and existing attrs
  t = Source$reshape(list('ac', nfa = 'strlen(ad)'), dtypes = list(nfa='int32'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('ac', 'nfa'))
  expect_identical(t$get_field_types(), list(z = 'int64', ac = 'dtac', nfa = 'int32'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad)), ac, nfa)")
  
  # Select new fields and existing dims
  t = Source$reshape(list(nfa = 'strlen(ad)', 'da'), dtypes = list(nfa='int32'), dim_mode = 'drop', artificial_field = 'z')
  expect_identical(t$dims, 'z')
  expect_identical(t$attrs, c('nfa', 'da'))
  expect_identical(t$get_field_types(), list(z = 'int64', nfa = 'int32', da = 'dtda'))
  assert_afl_equal(t$to_afl(), "project(apply(unpack(s, z), nfa, strlen(ad)), nfa, da)")
})

test_that("Fields should follow the order they are provided", {
  Source = newArrayOp("s", c("da", "db"), c("ac", "ad"), dtypes = list(ac='dtac', ad='dtad', da='dtda', db='dtdb'),
                      dim_specs = list(da="0:1:2:3"))
  # Existing fileds should be in order
  assert_afl_equal(str(Source$reshape(list('ad', 'ac'))), "project(s, ad, ac) <ad:dtad, ac:dtac> [da=0:1:2:3; db]")
  assert_afl_equal(str(Source$reshape(list('ac', 'ad'))), "project(s, ac, ad) <ac:dtac, ad:dtad> [da=0:1:2:3; db]")
  
  # Extra fields should be in order
  assert_afl_equal(str(Source$reshape(list('extra'=42, 'ac'), dtypes = list('extra'='int32 NULL'))),
                   "project(apply(s, extra, 42), extra, ac) <extra:int32 NULL, ac:dtac> [da=0:1:2:3; db]")
  assert_afl_equal(str(Source$reshape(list('ad', 'extra'=42, 'ac'), dtypes = list('extra'='int32 NULL'))),
                   "project(apply(s, extra, 42), ad, extra, ac) <ad:dtad, extra:int32 NULL, ac:dtac> [da=0:1:2:3; db]")
  
  # Renamed fileds should be in order
  assert_afl_equal(str(Source$reshape(list('ad', 'ac'=42))),
                   "project(apply(project(apply(s, _ac, 42), ad, _ac), ac, _ac), ad, ac)
                   <ad:dtad, ac:dtac> [da=0:1:2:3; db]")
  assert_afl_equal(str(Source$reshape(list('ac'=42, 'ad'))),
                   "project(apply(project(apply(s, _ac, 42), ad, _ac), ac, _ac), ac, ad)
                   <ac:dtac, ad:dtad> [da=0:1:2:3; db]")
  
  assert_afl_equal(
    str(Source$reshape(list('extra'=42, 'ac', 'db', 'da'), dtypes = list('extra'='int32 NULL'), 
                       dim_mode = 'drop', artificial_field = 'z')),
    "project(apply(unpack(s, z), extra, 42), extra, ac, db, da) <extra:int32 NULL, ac:dtac, db:dtdb, da:dtda> [z]")
  assert_afl_equal(
    str(Source$reshape(list('extra'="double(42)", 'ac', 'db'='int64(da+db)'), dtypes = list('extra'='int32 NULL', 'db'='int64 NULL'), 
                       dim_mode = 'drop', artificial_field = 'z')),
    "
    project(
      apply(
        project(
          apply(
            unpack(s, z), 
            _db, int64(da+db)), 
          ac, _db), 
        db, _db, extra, double(42)),
        extra, ac, db
    ) 
    <extra:int32 NULL, ac:dtac, db:int64 NULL> [z]")
})


# Build new -------------------------------------------------------------------------------------------------------

Template = newArrayOp('template', c('da', 'db'), c('aa', 'ab', 'ac'), 
  dtypes = list(da='int64', db='int64', aa='string', ab='int32', ac='bool'))

test_that("New ArrayOp from building a data.frame with full field match", {
  # Build operator accepts compound attribute types
  templateWithCompoundAttrs = newArrayOp('template', c('da', 'db'), c('aa', 'ab', 'ac'), 
    dtypes = list(da='int64', db='int64 null', aa='string not null', ab='int32', ac='bool'))
  df = data.frame(da = c(1,2), aa=c('aa1', "a' \" \\ a2"), ab=c(3,4), db = c(5, 6), ac = c(T, F))
  built = templateWithCompoundAttrs$build_new(df, artificial_field = 'z')
  assert_afl_equal(built$to_afl(),
    "build(<da:int64, aa:string not null, ab:int32, db:int64 null, ac:bool>[z],
        '[(1, \\'aa1\\', 3, 5, true), (2, \\'a\\\\' \" \\\\\\ a2\\', 4, 6, false)]', true)")
  expect_identical(built$attrs, c('da', 'aa', 'ab', 'db', 'ac'))
  expect_identical(built$get_field_types(built$attrs), templateWithCompoundAttrs$get_field_types(built$attrs))
})

test_that("New ArrayOp from building a data.frame with partial field match", {
  df = data.frame(aa=c('aa1', 'aa2'), ab=c(3,4))
  built = Template$build_new(df, artificial_field = 'z')
  assert_afl_equal(built$to_afl(),
    "build(<aa:string, ab:int32>[z],
        '[(\\'aa1\\', 3), (\\'aa2\\', 4)]', true)")
  expect_identical(built$attrs, c('aa', 'ab'))
  expect_identical(built$get_field_types(built$attrs), Template$get_field_types(built$attrs))
})

test_that("Build with a customized dimension", {
  df = data.frame(aa=c('aa1', 'aa2'), ab=c(3,4))
  built = Template$build_new(df, artificial_field = 'z=5:*')
  assert_afl_equal(built$to_afl(),
    "build(<aa:string, ab:int32>[z=5:*],
        '[(\\'aa1\\', 3), (\\'aa2\\', 4)]', true)")
  built = Template$build_new(df, artificial_field = 'z=1:*:0:1234')
  assert_afl_equal(built$to_afl(),
    "build(<aa:string, ab:int32>[z=1:*:0:1234],
        '[(\\'aa1\\', 3), (\\'aa2\\', 4)]', true)")
})

test_that("New ArrayOp from building a data.frame with NULL values", {
  template = newArrayOp('template', 'da', c('aa', 'ab'), dtypes = list(da='int64', aa='string', ab='int32'))
  
  df = data.frame(da = c(1,2, NA, 4), aa=c('aa1', NA, NA, 'aa2'), ab=c(NA,4,NA, 5))
  built = template$build_new(df, artificial_field = 'xxx')
  
  assert_afl_equal(built$to_afl(),
    "build(<da:int64, aa:string, ab:int32>[xxx],
                    '[(1, \\'aa1\\',), (2,, 4), (,,), (4, \\'aa2\\', 5)]', true)")
})

test_that("All colum names in df should be valid template fields", {
  template = newArrayOp('template', 'da', c('aa', 'ab'), dtypes = list(da='int64', aa='string', ab='int32'))
  df = data.frame(da = 1, extra = 2)
  expect_error(template$build_new(df),  "not found in template")
})


# Spawn -----------------------------------------------------------------------------------------------------------

test_that("Spawn a new ArrayOp from a template", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='int32'))
  spawned = t$spawn()
  expect_identical(spawned$dims, c('da', 'db'))
  expect_identical(spawned$attrs, c('aa', 'ab'))
  expect_identical(spawned$get_field_types(), list(da='int64', db='int64', aa='string', ab='int32'))
  assert_afl_equal(spawned$to_schema_str(), "<aa:string, ab:int32> [da;db]")
  
  spawned = t$spawn(excluded = c('da', 'ab'))
  expect_identical(spawned$dims, c('db'))
  expect_identical(spawned$attrs, c('aa'))
  expect_identical(spawned$get_field_types(), list(db='int64', aa='string'))
  assert_afl_equal(spawned$to_schema_str(), "<aa:string> [db]")
  
  spawned = t$spawn(renamed = list(da = 'a', ab = 'AB'))
  expect_identical(spawned$dims, c('a', 'db'))
  expect_identical(spawned$attrs, c('aa', 'AB'))
  expect_identical(spawned$get_field_types(), list(a='int64', db='int64', aa='string', AB='int32'))
  assert_afl_equal(spawned$to_schema_str(), "<aa:string, AB:int32> [a;db]")
  
  spawned = t$spawn(renamed = list(da = 'a', ab = 'AB'), excluded = 'aa')
  expect_identical(spawned$dims, c('a', 'db'))
  expect_identical(spawned$attrs, c('AB'))
  expect_identical(spawned$get_field_types(), list(a='int64', db='int64', AB='int32'))
  assert_afl_equal(spawned$to_schema_str(), "<AB:int32> [a;db]")
})

test_that("Spawn a new ArrayOp with extra fields", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='int32'),
    dim_specs = list(da = '0:1:0:1', db = '1:*:0:24'))
  
  spawned = t$spawn(added = c('extra1', 'extra2'), dtype = list(extra1 = 'int64', extra2 = 'string'))
  expect_identical(spawned$dims, c('da', 'db'))
  expect_identical(spawned$attrs, c('aa', 'ab', 'extra1', 'extra2'))
  expect_identical(spawned$get_field_types(), list(da='int64', db='int64', aa='string', ab='int32', 
    extra1 = 'int64', extra2 = 'string'))
  
  spawned = t$spawn(added = c('extra1', 'extra2'), renamed = list(db='x'), excluded = c('da', 'aa'), 
    dtype = list(extra1 = 'int64', extra2 = 'string'), dim_specs = list('extra1' = '0:*:0:*'))
  expect_identical(spawned$dims, c('x', 'extra1'))
  expect_identical(spawned$attrs, c('ab', 'extra2'))
  expect_identical(spawned$get_field_types(), list(x='int64', extra1 = 'int64', ab='int32', extra2 = 'string'))
  expect_identical(spawned$get_dim_specs(), list(x='1:*:0:24', extra1 = '0:*:0:*'))
  
})

# To schema str ---------------------------------------------------------------------------------------------------

test_that("Output a schema representation for the ArrayOp", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa="string compression 'zlib'", ab='int32 not null'))
  assert_afl_equal(t$to_schema_str(), "<aa:string compression 'zlib', ab:int32 not null> [da;db]")
  assert_afl_equal(t$spawn(excluded = c('db'))$to_schema_str(), "<aa:string compression 'zlib', ab:int32 not null> [da]")
  assert_afl_equal(t$spawn(excluded = c('aa'))$to_schema_str(), "<ab:int32 not null> [da; db]")
})

test_that("Output a schema representation for the ArrayOp with dimension specs", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='int32'),
                 dim_specs = list(da = '0:*:0:*', db='1:23:0:23'))
  assert_afl_equal(t$to_schema_str(), "<aa:string, ab:int32> [da=0:*:0:*;db=1:23:0:23]")
})


# create, delete, delete_versions ---------------------------------------------------------------------------------

test_that("Common array operations", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='int32'),
    dim_specs = list(da='1:24:0:1', db='*'))
  assert_afl_equal(t$create_array_cmd('NEW'), "create array NEW <aa:string, ab:int32> [da=1:24:0:1; db=*]")
  assert_afl_equal(t$remove_array_cmd(), "remove(t)")
  assert_afl_equal(t$remove_array_versions_cmd(), "remove_versions(t)")
  assert_afl_equal(t$remove_array_versions_cmd(version_id = NULL), "remove_versions(t)")
  assert_afl_equal(t$remove_array_versions_cmd(version_id = 3), "remove_versions(t, 3)")
})


# To join operand -------------------------------------------------------------------------------------------------
# Tests on the joined result are scidb version specific because the syntax is different in v19 than v18

test_that("ArrayOp as an operand in a join", {
  t = newArrayOp('t', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='int32'))
  
  # No selected fields, always use the array name as join operand
  assert_afl_equal(t$.to_join_operand_afl('da'), 't')
  assert_afl_equal(t$.to_join_operand_afl('aa'), 't')
  assert_afl_equal(t$.to_join_operand_afl('db', keep_dimension = T), 't')
  assert_afl_equal(t$.to_join_operand_afl('aa', keep_dimension = T), 't')
  
  # With selected fields but not keep dimensions
  assert_afl_equal(t$select('aa')$.to_join_operand_afl('da'), 'project(t, aa)')
  assert_afl_equal(t$select('db')$.to_join_operand_afl('da'), 'project(apply(t, db, db), db)')
  assert_afl_equal(t$select('ab')$.to_join_operand_afl('aa'), 'project(t, ab, aa)')
  assert_afl_equal(t$select('db')$.to_join_operand_afl('aa'), 'project(apply(t, db, db), db, aa)')
  
  # With selected fields and keep dimensions
  assert_afl_equal(t$select('aa')$.to_join_operand_afl('da', keep_dimension = T), 'project(t, aa)')
  assert_afl_equal(t$select('db')$.to_join_operand_afl('da', keep_dimension = T, artificial_field = 'x'),
    'project(apply(t, x, null), x)')
  assert_afl_equal(t$select('ab')$.to_join_operand_afl('aa', keep_dimension = T), 'project(t, ab, aa)')
  assert_afl_equal(t$select('db')$.to_join_operand_afl('aa', keep_dimension = T), 'project(t, aa)')
})

test_that("to_join_operand_afl keep_dimension = F", {
  t = newArrayOp('t', c('da', 'db', 'dc'), c('aa', 'ab', 'ac'), 
    dtypes = list(da='int64', db='int64', dc='int64', aa='string', ab='int32', ac='bool'))
  
  test_select_key = function(selected, key, afl_str){
    assert_afl_equal(t$select(selected)$.to_join_operand_afl(key, artificial_field='x'), afl_str)
  }
  
  test_select_key('aa', 'aa', 'project(t, aa)')
  test_select_key('da', 'da', 'project(apply(t, x, null), x)')
  test_select_key('aa', 'da', 'project(t, aa)')
  test_select_key('db', 'da', 'project(apply(t, db, db), db)')
  test_select_key('ab', 'aa', 'project(t, ab, aa)')
  test_select_key('db', 'aa', 'project(apply(t, db, db), db, aa)')
})

test_that("to_join_operand_afl keep_dimension = T", {
  t = newArrayOp('t', c('da', 'db', 'dc'), c('aa', 'ab', 'ac'), 
    dtypes = list(da='int64', db='int64', dc='int64', aa='string', ab='int32', ac='bool'))
  
  test_select_key = function(selected, key, afl_str){
    assert_afl_equal(t$select(selected)$.to_join_operand_afl(key, keep_dimension = T, artificial_field='x'), afl_str)
  }
  
  test_select_key('aa', 'aa', 'project(t, aa)')
  test_select_key('da', 'da', 'project(apply(t, x, null), x)')
  test_select_key('aa', 'da', 'project(t, aa)')
  test_select_key('db', 'da', 'project(apply(t, x, null), x)')
  test_select_key('ab', 'aa', 'project(t, ab, aa)')
  test_select_key('db', 'aa', 'project(t, aa)')
})


# Join - cross_join mode ------------------------------------------------------------------------------------------

test_that("Join cross_join mode", {
  left = newArrayOp('left', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  right = newArrayOp('right', c('rda', 'rdb'), c('raa', 'rab'), dtypes = list(rda='int64', rdb='int64', raa='string', rab='double'))
  joined = left$join(right, on_left = 'da', on_right = 'rda', join_mode = 'cross_join')
  assert_afl_equal(joined$to_afl(), "cross_join(left as _L, right as _R, _L.da, _R.rda)")
  expect_identical(joined$dims, c('da', 'db', 'rdb'))
  expect_identical(joined$attrs, c('aa', 'ab', 'raa', 'rab'))
  expect_identical(joined$get_field_types(joined$dims), list(da='int64', db='int64', rdb='int64'))
  expect_identical(joined$get_field_types(joined$attrs), list(aa='string', ab='double', raa='string', rab='double'))
})

test_that("on_both shorthand for on_left and on_right", {
  left = newArrayOp('left', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  right = newArrayOp('right', c('da', 'db'), c('raa', 'rab'), dtypes = list(da='int64', db='int64', raa='string', rab='double'))
  joined = left$join(right, on_both='da', join_mode = 'cross_join')
  assert_afl_equal(joined$to_afl(), "cross_join(left as _L, right as _R, _L.da, _R.da)")
  expect_identical(joined$dims, c('da', 'db', 'db'))
  expect_identical(joined$attrs, c('aa', 'ab', 'raa', 'rab'))
  expect_identical(joined$get_field_types(joined$dims), list(da='int64', db='int64', db='int64'))
  expect_identical(joined$get_field_types(joined$attrs), list(aa='string', ab='double', raa='string', rab='double'))
})

test_that("Set operand aliases", {
  left = newArrayOp('left', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  right = newArrayOp('right', c('da', 'db'), c('raa', 'rab'), dtypes = list(da='int64', db='int64', raa='string', rab='double'))
  joined = left$join(right, on_both='da', join_mode = 'cross_join', .left_alias = 'L', .right_alias = 'R')
  assert_afl_equal(joined$to_afl(), "cross_join(left as L, right as R, L.da, R.da)")
  expect_identical(joined$dims, c('da', 'db', 'db'))
  expect_identical(joined$attrs, c('aa', 'ab', 'raa', 'rab'))
  expect_identical(joined$get_field_types(joined$dims), list(da='int64', db='int64', db='int64'))
  expect_identical(joined$get_field_types(joined$attrs), list(aa='string', ab='double', raa='string', rab='double'))
})

test_that("Cannot cross_join on attributes (dimensions only)", {
  left = newArrayOp('left', c('da', 'db'), c('aa', 'ab'), dtypes = list(da='int64', db='int64', aa='string', ab='double'))
  right = newArrayOp('right', c('da', 'db'), c('raa', 'rab'), dtypes = list(da='int64', db='int64', raa='string', rab='double'))
  expect_error(left$join(right, on_left='aa', on_right='raa', join_mode = 'cross_join'), "must be dimensions")
})

# Set null fields ---------------------------------------------------------------------------------------

test_that("Set null fields to a source according to a ref", {
  source = newArrayOp('source', 'x', c('b', 'c', 'd'), dtypes = list(a='int64', b='string', c='bool', d='double', x='int64'))
  ref = newArrayOp('ref', 'dim', c('a', 'b', 'c', 'd'), dtypes = list(a='int64 null', b='string', c='bool', d='double', dim='int64'))
  # Dimension will not be automatically referenced
  result = source$set_null_fields(ref)
  assert_afl_equal(result$to_afl(), "apply(source, a, int64(null))")
  assert_afl_equal(result$to_schema_str(), "<b:string,c:bool,d:double,a:int64> [x]")
  
  result = source$set_null_fields(list(a = 'int64', a2 = 'string'))
  assert_afl_equal(result$to_afl(), 
                   "apply(source, a, int64(null), a2, string(null))")
  assert_afl_equal(result$to_schema_str(), "<b:string,c:bool,d:double,a:int64,a2:string> [x]")
  
  # Passing an explicit list can attach any new field to the soruce
  result = source$set_null_fields(ref$get_field_types(c('dim', 'a'), .raw = T))
  assert_afl_equal(result$to_afl(), 
                   "apply(source, dim, int64(null), a, int64(null))")
  assert_afl_equal(result$to_schema_str(), "<b:string,c:bool,d:double,dim:int64,a:int64> [x]")
})
# Set auto increment fields ---------------------------------------------------------------------------------------
# apply auto incremnt id to the Source with incremented values from the reference

test_that("Auto increment the reference dimension", {
  source = newArrayOp('s', 'x', 'a')
  ref = newArrayOp('ref', c('da', 'db', 'dc'), c('aa', 'ab', 'ac'))
  
  # new_field defaults to the reference fields
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = 'da', source_start = 5, ref_start = 2)
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(apply(ref, da, da), max(da) as _max_da)
    ), 
    da,  iif(_max_da is null, x-3, _max_da + x - 4)
  )")
  # new_field can be set explicitly
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = 'da', source_start = 5, ref_start = 2, new_field = 'anything')
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(apply(ref, da, da), max(da) as _max_da)
    ), 
    anything,  iif(_max_da is null, x-3, _max_da + x - 4)
  )")
})

test_that("More than one auto increment field is allowed", {
  source = newArrayOp('s', 'x', 'a', dtypes = list(a='string', x='int64'))
  ref = newArrayOp('ref', c('da', 'db', 'dc'), c('aa', 'ab', 'ac'), dtypes = list(ab='int32', da='int64'))
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = c('da', 'ab'), source_start = 0, ref_start = c(5, 3))
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(apply(ref, da, da), max(da) as _max_da, max(ab) as _max_ab)
    ), 
    da,  iif(_max_da is null, x+5, _max_da + x + 1),
    ab,  iif(_max_ab is null, x+3, _max_ab + x + 1)
  )")
  expect_identical(result$dims, 'x')
  expect_identical(result$attrs, c('a', 'da', 'ab'))
  expect_identical(result$get_field_types(result$attrs), list('a'='string', 'da'='int64', 'ab'='int32'))
})

test_that("Auto increment the reference attribute", {
  source = newArrayOp('s', 'x', 'a')
  ref = newArrayOp('ref', c('da', 'db', 'dc'), c('aa', 'ab', 'ac'))
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = 'aa', source_start = 0, ref_start = 1, new_field = 'aa')
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(ref, max(aa) as _max_aa)
    ), 
    aa,  iif(_max_aa is null, x + 1, _max_aa + x + 1)
  )")
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = 'aa', source_start = 0, ref_start = 0, new_field = 'aa')
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(ref, max(aa) as _max_aa)
    ), 
    aa,  iif(_max_aa is null, x, _max_aa + x + 1)
  )")
  result = source$set_auto_increment_field(ref, 
    source_field = 'x', ref_field = 'aa', source_start = 5, ref_start = 2, new_field = 'aa')
  assert_afl_equal(result$to_afl(), "
  apply(
    cross_join(
      s,
      aggregate(ref, max(aa) as _max_aa)
    ), 
    aa,  iif(_max_aa is null, x-3, _max_aa + x - 4)
  )")
})




# Mutate array content ----

# Data source from a named list 

Target = newArrayOp("Target", dims = c('da', 'db', 'dc'), attrs = c('aa', 'ab', 'ac'), 
                    dtypes = list('da'='int64', 'db'='int64', 'dc'='int64', 'aa'='string zip', 'ab'='bool', 'ac'='double nullable'),
                    dim_specs = list('da' = 'da_spec', 'db' = 'db_spec', 'dc' = 'dc_spec'))
test_that("Mutate array content by providing mutated attr expressions", {
  mutated = Target$mutate(list('aa' = "'mutated'"))
  assert_afl_equal(mutated$to_afl(), 
                   "project(apply(project(apply(Target, _aa, 'mutated'), ab, ac, _aa), aa, _aa), aa, ab, ac)")
  mutated = Target$mutate(list('ac' = 'double(null)', 'aa' = "'mutated'"))
  assert_afl_equal(mutated$to_afl(), 
                   "project(apply(project(apply(Target, _ac, double(null), _aa, 'mutated'), ab, _ac, _aa), ac, _ac, aa, _aa), aa, ab, ac)")
  
})

test_that("Mutate array content by providing mutated dimension expressions", {
  mutated = Target$mutate(list('da' = 42L), artificial_field = 'z')
  assert_afl_equal(mutated$to_afl(), 
     "
  redimension(
     project(
        apply(
          project(
            apply(unpack(Target, z), _da, 42), 
            db, dc, aa, ab, ac, _da),
          da, _da),
        da, db, dc, aa, ab, ac),
  <aa:string zip, ab: bool, ac:double nullable> [da=da_spec; db=db_spec; dc=dc_spec])")
  mutated = Target$where(ac > 3.14 && aa == 'pi')$mutate(list('aa' = "'PI'", 'da' = 42L), artificial_field = 'z')
  assert_afl_equal(mutated$to_afl(), 
     "
  redimension(
     project(
        apply(
          project(
            apply(unpack(filter(Target, ac > 3.14 and aa = 'pi'), z), _aa, 'PI', _da, 42), 
            db, dc, ab, ac, _aa, _da),
          aa, _aa, da, _da),
        da, db, dc, aa, ab, ac),
  <aa:string zip, ab: bool, ac:double nullable> [da=da_spec; db=db_spec; dc=dc_spec])")
})

# Data source from an arrayOp instance

Target = ArrayOpV19$new("Target", dims = c('da', 'db', 'dc'), attrs = c('aa', 'ab', 'ac'), 
                    dtypes = list('da'='int64', 'db'='int64', 'dc'='int64', 'aa'='string zip', 'ab'='bool', 'ac'='double nullable'),
                    dim_specs = list('da' = 'da_spec', 'db' = 'db_spec', 'dc' = 'dc_spec'))

test_that("Mutate array content by an arrayOp data source with full dimensions", {
  ds = newArrayOp("DS", Target$dims, attrs = c('aa', 'ac'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds)
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      DS,
      project(Target, ab)
    ), 
    aa, ab, ac)
  ")
  ds = newArrayOp("DS", Target$dims, attrs = c('ac', 'aa'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds)
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      DS,
      project(Target, ab)
    ), 
    aa, ab, ac)
  ")
  # Extra fields in data source
  ds = newArrayOp("DS", Target$dims, attrs = c('ac', 'aa', 'extra'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds)
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      project(DS, ac, aa),
      project(Target, ab)
    ), 
    aa, ab, ac)
  ")
  ds = newArrayOp("DS", Target$dims, attrs = c('ab'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds)
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      DS,
      project(Target, aa, ac)
    ), 
    aa, ab, ac)
  ")
})

test_that("Mutate array content from an arrayOp data source with partial dimensions", {
  ds = ArrayOpV19$new("DS", dims = c('da', 'db'), attrs = c('aa', 'ac'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds, keys = c('da', 'db', 'aa'), updated_fields = 'ac')
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      redimension(
        project(
          equi_join(
            project(DS, ac, aa) as _L, 
            project(apply(Target, dc, dc), dc, aa) as _R, 
              left_names:(_L.da, _L.db, _L.aa), right_names:(_R.da, _R.db, _R.aa)),
          ac, da, db, dc),
        <ac:double nullable> [da=da_spec; db=db_spec; dc=dc_spec]),
      project(Target, aa, ab)
    ), 
    aa, ab, ac)
  ")
})

test_that("Mutate array content from an arrayOp data source whose attrs include all target dimensions", {
  ds = ArrayOpV19$new("DS", dims = c('x'), attrs = c('da', 'db', 'dc', 'ab'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds, keys = c('da', 'db', 'dc'), updated_fields = c('ab'))
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      redimension(
          DS,
          <ab:bool> [da=da_spec; db=db_spec; dc=dc_spec]),
      project(Target, aa, ac)
    ), 
    aa, ab, ac)
  ")
  ds = ArrayOpV19$new("DS", dims = c('x'), attrs = c('da', 'db', 'dc', 'ab', 'ac'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  mutated = Target$mutate(ds, keys = c('da', 'db', 'dc'), updated_fields = c('ab'))
  assert_afl_equal(mutated$to_afl(),
  "project(
    join(
      redimension(
          project(DS,da, db, dc,ab),
          <ab:bool> [da=da_spec; db=db_spec; dc=dc_spec]),
      project(Target, aa, ac)
    ), 
    aa, ab, ac)
  ")
})


 
# Update array content with partial attributes/dimensions ----
# 
# Use case: 
# A target array with <aa, ab, ac, ad> [da; db; dc]
# A data source array with a subset of target's dims_n_attrs
# Expected behavior:
# Target array is updated with Source's data. 
# Only the content of matched variables is updated; unmatched variables remains the same
# 

Target = newArrayOp("Target", dims = c('da', 'db', 'dc'), attrs = c('aa', 'ab', 'ac'), 
                    dtypes = list('da'='int64', 'db'='int64', 'dc'='int64', 'aa'='string zip', 'ab'='bool', 'ac'='double nullable'),
                    dim_specs = list('da' = 'da_spec', 'db' = 'db_spec', 'dc' = 'dc_spec'))

test_that("Update with matching dimensions and matching attributes", {
  source = newArrayOp("Source", c('da', 'db', 'dc'), c('aa', 'ab', 'ac'), dtypes = Target$get_field_types(), dim_specs = Target$get_dim_specs())
  # If all variable types match, their names do not matter
  source2 = newArrayOp("Source", c('xda', 'xdb', 'xdc'), c('xaa', 'xab', 'xac'), 
                       dtypes = list('xda'='int64', 'xdb'='int64', 'xdc'='int64', 'xaa'='string zip', 'xab'='bool', 'xac'='double nullable'),
                       dim_specs = list('xda' = 'da_spec', 'xdb' = 'db_spec', 'xdc' = 'dc_spec'))
  op = Target$update_by(source)
  op2 = Target$update_by(source2)
  assert_afl_equal(op$to_afl(), "insert(Source, Target)")
  assert_afl_equal(op2$to_afl(), "insert(Source, Target)")
})

test_that("Update with 'where' clause ", {
  source = Target$where(da > 2 && aa == 'string')$mutate(list('ac' = 42))
  op = Target$update_by(source)
  assert_afl_equal(op$to_afl(), 
  "insert(
    project(
      apply(
        project(
          apply(
            filter(Target, da > 2 and aa = 'string'),
            _ac, 42), 
          aa, ab, _ac),
        ac, _ac),
      aa, ab, ac),
  Target)")
})

