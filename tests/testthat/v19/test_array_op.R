context("Scidb V19.x ArrayOp")

# aio_input -------------------------------------------------------------------------------------------------------

test_that("Load array from file", {
  template = newArrayOp('L', c('da', 'db'), c('aa', 'ab'), 
    dtypes = list(da='int64', db='int64', aa="string compression 'zlib'", ab='double not null'))
  for(loaded  in c(
    template$load_file('file_path'),
    template$load_file('file_path', file_headers = c('da', 'db', 'aa', 'ab'))
  )){
    assert_afl_equal(loaded$to_afl(),
      "project(
        apply(
          aio_input('file_path', num_attributes:4),
          da, int64(a0), db, int64(a1), aa, a2, ab, double(a3)
        ),
      da, db, aa, ab)")
  }
  assert_afl_equal(
    template$load_file('file_path', file_headers = c('aa', 'ab'))$to_afl(),
    "project(
        apply(
          aio_input('file_path', num_attributes:2),
          aa, a0, ab, double(a1)
        ),
    aa, ab)"
  )
  # apply/project'ed fields order follows the template schema, ie. dims + attrs
  assert_afl_equal(
    template$load_file('file_path', file_headers = c('aa', 'db'))$to_afl(),
    "project(
        apply(
          aio_input('file_path', num_attributes:2),
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
        aio_input('file_path', num_attributes:5),
        da, int64(a1), db, int64(a2), aa, a3, ab, double(a4)
      ),
    da, db, aa, ab)")
  
  loaded = template$load_file('file_path', file_headers = c('da', 'skip', 'skip', 'db'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('file_path', num_attributes:4),
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
        aio_input('file_path', num_attributes:6),
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
        aio_input('file_path', num_attributes:6, header:1, attribute_delimiter:'\\t'),
        da, int64(a0), db, int64(a1), aa, a2, ab, double(a3), ac, int32(a4), ad, bool(a5)
      ),
    da, db, aa, ab, ac, ad)")
  # Pass the delimiter by literal
  loaded = template$load_file('file_path', aio_settings = list(header = 1, attribute_delimiter = '\t'))
  assert_afl_equal(loaded$to_afl(),
    "project(
      apply(
        aio_input('file_path', num_attributes:6, header:1, attribute_delimiter:'\t'),
        da, int64(a0), db, int64(a1), aa, a2, ab, double(a3), ac, int32(a4), ad, bool(a5)
      ),
    da, db, aa, ab, ac, ad)")
})


# Write to with anti collision id ---------------------------------------------------------------------------------

test_that("Write a dataset ArrayOp to target in redimension mode with anti-collision field", {
  # When regular dimensions can overlap, we need an artificial dimension to make each cell coordinate unique
  # which is named anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'da', 'ab'), 
                  dtypes = list(x='int64', db='int64', aa='string', da='int32', ab='int32'))
  
  writeOp = ds$write_to(Target, anti_collision_field = 'altid')
  assert_afl_equal(writeOp$to_afl(), 
                   "insert(redimension(
      apply(
        equi_join(
          apply(
            redimension(
              dataset,
              <aa:string, ab:int32>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              target,
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )
    ,target), target)")
})

test_that("Write a dataset ArrayOp to target in redimension mode with both auto-increment-id and anti-collision-field", {
  # When regular dimensions can overlap, we need an artificial dimension to make each cell coordinate unique
  # which is named anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab', 'aid'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32', aid='int64'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'aa', 'da', 'ab'), 
                  dtypes = list(x='int64', db='int64', aa='string', da='int32', ab='int32'))
  
  writeOp = ds$write_to(Target, anti_collision_field = 'altid', 
                        source_auto_increment = c(x = 0), target_auto_increment = c(aid = 1))
  
  autoIncremented = 
    "apply(
        cross_join(
          dataset,
          aggregate(
            target, max(aid) as _max_aid)
        ),
        aid, iif(_max_aid is null, x + 1, _max_aid + x + 1)
    )"
  
  assert_afl_equal(writeOp$to_afl(), sprintf(
    "insert(redimension(
      apply(
        equi_join(
          apply(
            redimension(
              %s,
              <aa:string, ab:int32, aid:int64>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              target,
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )
    ,target), target)", autoIncremented))
})

# Set auto fields ---------------------------------------------------------------------------------

test_that("Set anti-collision field", {
  # When regular dimensions can overlap, we need an artificial dimension to make each cell coordinate unique
  # which is named anti-collision field
  # The source array should have all dimensions as the target except for the anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'dsa', 'da'), 
                  dtypes = list(x='int64', db='int64', dsa='string', da='int32'))
  
  writeOp = ds$set_auto_fields(Target, anti_collision_field = 'altid')
  assert_afl_equal(writeOp$to_afl(), "
      apply(
        equi_join(
          apply(
            redimension(
              dataset,
              <dsa:string>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              target,
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )")
  assert_afl_equal(writeOp$to_schema_str(), "<dsa:string> [da=0:*:0:1; db=0:*:0:*; altid=0:*:0:1234]")
})

test_that("Set anti-collision field with a customized _src_altid dimension spec", {
  # By default, source array has a synthetic _src_altid dimension that shares the target's anti-collision dimension spec.
  # This can be overriden by a customized dimension spec, which is useful when the source array has potentially more
  # conflicting dimension coordinates than the Target's anti-collision dimension chunk size.
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'dsa', 'da'), 
                  dtypes = list(x='int64', db='int64', dsa='string', da='int32'))
  
  writeOp = ds$set_auto_fields(Target, anti_collision_field = 'altid', source_anti_collision_dim_spec = "0:*:0:*")
  assert_afl_equal(writeOp$to_afl(), "
      apply(
        equi_join(
          apply(
            redimension(
              dataset,
              <dsa:string>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:*]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              target,
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )")
  assert_afl_equal(writeOp$to_schema_str(), "<dsa:string> [da=0:*:0:1; db=0:*:0:*; altid=0:*:0:*]")
})

test_that("Set anti-collision field according to a reshaped target", {
  # Only target's dimensions are necessary, so we can safely project away other attributes using 'reshape'
  # Target array may have a large number of attributes, which can slow down the join
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab', 'ac', 'ad'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32', ac='bool', ad='string'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'dsa', 'da'), 
                  dtypes = list(x='int64', db='int64', dsa='string', da='int32'))
  
  writeOp = ds$set_auto_fields(Target$reshape(Target$dims, artificial_field = 'xxx'), 
                               anti_collision_field = 'altid')
  assert_afl_equal(writeOp$to_afl(), "
      apply(
        equi_join(
          apply(
            redimension(
              dataset,
              <dsa:string>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              project(apply(target, xxx, null), xxx),
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )")
  assert_afl_equal(writeOp$to_schema_str(), "<dsa:string> [da=0:*:0:1; db=0:*:0:*; altid=0:*:0:1234]")
})

test_that("Set anti-collision field with custom join settings", {
  # When regular dimensions can overlap, we need an artificial dimension to make each cell coordinate unique
  # which is named anti-collision field
  # The source array should have all dimensions as the target except for the anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'dsa', 'da'), 
                  dtypes = list(x='int64', db='int64', dsa='string', da='int32'))
  
  writeOp = ds$set_auto_fields(Target, anti_collision_field = 'altid', join_setting = list(join_hash_threshold=1024))
  assert_afl_equal(writeOp$to_afl(), "
      apply(
        equi_join(
          apply(
            redimension(
              dataset,
              <dsa:string>
              [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
            ),
            _src_altid, _src_altid
          ) as _L,
          grouped_aggregate(
            apply(
              target,
              altid, altid
            ),
            max(altid) as _max_altid, da, db
          ) as _R,
          left_names:(_L.da,_L.db),
          right_names:(_R.da,_R.db),
          join_hash_threshold:1024,
          left_outer:1
        ),
        altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
      )")
  assert_afl_equal(writeOp$to_schema_str(), "<dsa:string> [da=0:*:0:1; db=0:*:0:*; altid=0:*:0:1234]")
})

test_that("Set anti-collision field (Missing dimension fields)", {
  # The source array should have all dimensions as the target except for the anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'aa'), 
                  dtypes = list(x='int64', db='int64', aa='string', da='int32'))
  expect_error(ds$set_auto_fields(Target, anti_collision_field = 'altid'), "incorrect number")
})

test_that("Set two auto-incremented fields", {
  Target = newArrayOp('target', c('da', 'altid'), c('taa', 'ab', 'aid', 'aid2'), 
                      dtypes = list(da='int64', db='int64', altid='int64', taa='string', ab='int32', aid='int32', aid2='int64'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('aa', 'ds1'), 
                  dtypes = list(x='int64', aa='string', ds1='int32'))
  
  writeOp = ds$set_auto_fields(Target, source_auto_increment = c(x = 0), target_auto_increment = c(aid = 1L, aid2=5))
  assert_afl_equal(writeOp$to_afl(), "
      apply(
        cross_join(
          dataset,
          aggregate(
            target, max(aid) as _max_aid, max(aid2) as _max_aid2
          )
        ),
        aid, iif(_max_aid is null, x + 1, _max_aid + x + 1),
        aid2, iif(_max_aid2 is null, x + 5, _max_aid2 + x + 1)
    )")
  assert_afl_equal(writeOp$to_schema_str(), "<aa:string, ds1:int32, aid:int32, aid2:int64> [x]")
  pruned = writeOp$reshape(writeOp$attrs)
  assert_afl_equal(pruned$to_afl(), sprintf("project(%s, aa, ds1, aid, aid2)", writeOp$to_afl()))
  assert_afl_equal(pruned$to_schema_str(), "<aa:string, ds1:int32, aid:int32, aid2:int64> [x]")
})

test_that("Set both auto-increment-id and anti-collision-field", {
  # When regular dimensions can overlap, we need an artificial dimension to make each cell coordinate unique
  # which is named anti-collision field
  Target = newArrayOp('target', c('da', 'db', 'altid'), c('aa', 'ab', 'aid'), 
                      dtypes = list(da='int64', db='int64', altid='int64', aa='string', ab='int32', aid='int64'),
                      dim_specs = list(da='0:*:0:1', db='0:*:0:*', altid='0:*:0:1234'))
  
  ds = newArrayOp('dataset', 'x', c('db', 'dsa', 'da', 'dsb'), 
                  dtypes = list(x='int64', db='int64', dsa='string', da='int32', dsb='int32'))
  
  # The 3 operations below are equivelant
  operations = list(
    ds$set_auto_fields(Target, anti_collision_field = 'altid', 
                       source_auto_increment = c(x = 2), target_auto_increment = c(aid = 5L))
    ,
    ds$set_auto_increment_field(Target, 'x', 'aid', 2, 5)$
      set_anti_collision_field(Target)
    ,
    ds$set_auto_fields(Target, source_auto_increment = c(x = 2), target_auto_increment = c(aid = 5L))$
      set_auto_fields(Target, anti_collision_field = 'altid')
  )
  
  autoIncremented = 
    "apply(
        cross_join(
          dataset,
          aggregate(
            target, max(aid) as _max_aid)
        ),
        aid, iif(_max_aid is null, x + 3, _max_aid + x - 1)
    )"
  for(op in operations){
    assert_afl_equal(op$to_afl(), sprintf("
        apply(
          equi_join(
            apply(
              redimension(
                %s,
                <dsa:string, dsb:int32, aid:int64>
                [da=0:*:0:1; db=0:*:0:*; _src_altid=0:*:0:1234]
              ),
              _src_altid, _src_altid
            ) as _L,
            grouped_aggregate(
              apply(
                target,
                altid, altid
              ),
              max(altid) as _max_altid, da, db
            ) as _R,
            left_names:(_L.da,_L.db),
            right_names:(_R.da,_R.db),
            left_outer:1
          ),
          altid, iif(_max_altid is null, _src_altid, _src_altid + _max_altid + 1)
        )", autoIncremented))
    assert_afl_equal(op$to_schema_str(), "<dsa:string, dsb:int32, aid:int64> [da=0:*:0:1; db=0:*:0:*; altid=0:*:0:1234]")
  }
})
