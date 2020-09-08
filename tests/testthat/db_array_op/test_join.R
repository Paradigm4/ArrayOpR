context("ArrayOp: join")


df_equal = function(actual_df, expected_df) {
  expect_equal(
    actual_df %>% dplyr::arrange(!!! sapply(names(actual_df), as.name)), 
    expected_df
  )
}

test_that("join with no conflicting field names", {
  leftDf = data.frame(lda = 1:5, ldb = 11:15, lfa = LETTERS[1:5],  lfb = 3.14 * 1:5)
  rightDf = data.frame(rda = 3:10, rdb = 13:20, rfa = LETTERS[3:10], rfb = 3.14 * 3:10)
  leftTemplate = conn$array_from_schema("<lfa:string, lfb:double> [lda;ldb] ")
  rightTemplate = conn$array_from_schema("<rfa:string, rfb:double> [rda;rdb] ")
  
  L = conn$array_from_df(leftDf, leftTemplate, force_template_schema = T)
  R = conn$array_from_df(rightDf, rightTemplate, force_template_schema = T)
  
  test_inner_join = function() {
    df_equal(
      L$inner_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('lfa'='rfa'))
    )
    
    df_equal(
      L$inner_join(R, on_left = 'lda', on_right = 'rda')$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('lda'='rda'))
    )
    
    df_equal(
      L$inner_join(R, on_left = c('lda', 'ldb'), on_right = c('rda', 'rdb'))$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('lda'='rda', 'ldb'='rdb'))
    )
    
    df_equal(
      L$inner_join(R, on_left = c('lfa', 'lfb'), on_right = c('rfa', 'rfb'))$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('lfa'='rfa', 'lfb'='rfb'))
    )
    
    df_equal(
      L$inner_join(R, on_left = c('lda', 'lfa'), on_right = c('rda', 'rfa'))$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('lda'='rda', 'lfa'='rfa'))
    )
  }
  
  # select fields before join
  test_select_fields = function() {
    
    df_equal(
      L$select('lfa', 'lfb')$inner_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      leftDf %>% dplyr::select(lfa, lfb) %>% dplyr::inner_join(rightDf, by = c('lfa'='rfa'))
    )
    
    df_equal(
      L$select('lfb')$inner_join(R$select('rfb'), on_left = 'lfa', on_right = 'rfa')$to_df(), 
      leftDf %>% 
        dplyr::select(lfa, lfb) %>% 
        dplyr::inner_join(rightDf %>% dplyr::select(rfa, rfb), 
                          by = c('lfa'='rfa')) %>%
        dplyr::select(lfb, rfb)
    )
    
    df_equal(
      L$select('ldb')$inner_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      leftDf %>% dplyr::select(lfa, ldb) %>% 
        dplyr::inner_join(rightDf, by = c('lfa'='rfa')) %>%
        dplyr::select(-lfa)
    )
    
    df_equal(
      L$select('lfb')$inner_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      leftDf %>% dplyr::select(lfa, lfb) %>% 
        dplyr::inner_join(rightDf, by = c('lfa'='rfa')) %>%
        dplyr::select(-lfa)
    )
    
    # Special cases where only dimensions are selcted and joined on
    df_equal(
      L$select('lda')$inner_join(R, on_left = 'lda', on_right = 'rda')$to_df(), 
      leftDf %>% dplyr::select(lda) %>% 
        dplyr::inner_join(rightDf, by = c('lda'='rda'))
    )
    df_equal(
      L$select('lda')$inner_join(R$select('rda'), on_left = 'lda', on_right = 'rda')$to_df(), 
      leftDf %>% dplyr::select(lda) %>% 
        dplyr::inner_join(
          rightDf %>% dplyr::select(rda), # rda will not be in the result
          by = c('lda'='rda')
        )
    )
    
  }
  
  test_select_fields_dry = function() {
    
    expect_identical(
      L$inner_join(R, on_left = 'lfa', on_right = 'rfa')$selected, 
      c('lda', 'ldb', 'lfa', 'lfb', 'rda', 'rdb', 'rfb')
    )
    expect_identical(
      L$inner_join(R$select('rda'), on_left = 'lfa', on_right = 'rfa')$selected, 
      c('lda', 'ldb', 'lfa', 'lfb', 'rda')
    )
    expect_identical(
      L$select('lfa', 'lfb')$inner_join(R, on_left = 'lfa', on_right = 'rfa')$selected, 
      c('lfa', 'lfb', 'rda', 'rdb', 'rfb')
    )
    expect_identical(
      L$select('lfa', 'lfb')$inner_join(R$select('rfb'), on_left = 'lfa', on_right = 'rfa')$selected, 
      c('lfa', 'lfb', 'rfb')
    )
    expect_identical(
      L$select('lfa', 'lfb')$inner_join(R$select('rfb', 'rfa'), on_left = 'lfa', on_right = 'rfa')$selected, 
      c('lfa', 'lfb', 'rfb')
    )
  }
  
  test_join_settings = function() {
    # scidb operator argument format has changed in scidb V19
    if(conn$query("op_scidbversion()")$major <= 18) return()

    expect_match(
      L$inner_join(R, on_left = 'lfa', on_right = 'rfa', settings = list(algorithm = "'hash_replicate_right'"))$to_afl(), 
      "algorithm:'hash_replicate_right'"
    )
    
    expect_match(
      L$inner_join(R, on_left = 'lfa', on_right = 'rfa', settings = list(keep_dimensions=1))$to_afl(), 
      "keep_dimensions:1"
    )
  }
  
  # left join
  test_left_join = function() {
    df_equal(
      L$left_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      dplyr::left_join(leftDf, rightDf, by = c('lfa'='rfa'))
    )
  }
  
  # right join
  test_right_join = function(){
    df_equal(
      L$right_join(R, on_left = 'lfa', on_right = 'rfa')$to_df(), 
      dplyr::right_join(leftDf, rightDf, by = c('lfa'='rfa'))
    )
  }
  
  test_inner_join()
  test_select_fields()
  test_select_fields_dry()
  test_join_settings()
  test_left_join()
  test_right_join()
  
  # L$remove_self()
  # R$remove_self()
})

test_that("join with conflicting field names", {
  leftDf = data.frame(lda = 1:5, db = 11:15, fa = LETTERS[1:5],  lfb = 3.14 * 1:5)
  rightDf = data.frame(rda = 3:10, db = 13:20, fa = LETTERS[3:10], rfb = 3.14 * 3:10)
  leftTemplate = conn$array_from_schema("<fa:string, lfb:double> [lda;db] ")
  rightTemplate = conn$array_from_schema("<fa:string, rfb:double> [rda;db] ")
  # At least one input to 'cross_join' must have a specified chunk size.
  # So we need to persist the arrays
  L = conn$array_from_df(leftDf, leftTemplate)$change_schema(leftTemplate)$persist(.gc = F)
  R = conn$array_from_df(rightDf, rightTemplate)$change_schema(rightTemplate)$persist(.gc = F)
  
  test_auto_join_keys = function() {
    df_equal(
      L$inner_join(R)$to_df(), # auto infer overlapping fields as join keys
      dplyr::inner_join(leftDf, rightDf)
    )
    df_equal(
      L$left_join(R)$to_df(), 
      dplyr::left_join(leftDf, rightDf)
    )
    df_equal(
      L$right_join(R)$to_df(), 
      dplyr::right_join(leftDf, rightDf)
    )
  }
  
  test_joins_with_conflicted_fields = function() {
    df_equal(
      L$inner_join(R, on_both = c('db', 'fa'))$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('fa', 'db') )
    )
    df_equal(
      L$inner_join(R, on_both = c('db'), left_alias = "_LL", right_alias = "_RR")$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('db'), suffix = c('_LL', '_RR'))
    )
    df_equal(
      L$left_join(R, on_both = c('db'), left_alias = "_LL", right_alias = "_RR")$to_df(), 
      dplyr::left_join(leftDf, rightDf, by = c('db'), suffix = c('_LL', '_RR'))
    )
    df_equal(
      L$right_join(R, on_both = c('db'), left_alias = "_LL", right_alias = "_RR")$to_df(), 
      dplyr::right_join(leftDf, rightDf, by = c('db'), suffix = c('_LL', '_RR'))
    )
    
    # No need to disambiguate fields if only one side is selected
    # here `fa` is only selected in left
    df_equal(
      L$select('fa')$inner_join(R$select('db'), on_left = c('db', 'lda'), on_right = c('db', 'rda'))$to_df(), 
      dplyr::inner_join(
        leftDf %>% dplyr::select(fa, db, lda), 
        rightDf %>% dplyr::select(db, rda), 
        by = c('db'='db', 'lda'='rda')) %>%
        dplyr::select(fa)
    )
    
  }
  
  test_cross_join_mode = function() {
    df_equal(
      L$inner_join(R, on_both = c('db'), join_mode = 'cross_join', left_alias = "_LL", right_alias = "_RR")$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('db'), suffix = c('_LL', '_RR'))
    )
    df_equal(
      L$inner_join(R, on_left = c('db', 'lda'), on_right = c('db', 'rda'), join_mode = 'cross_join', left_alias = "_LL", right_alias = "_RR")$to_df(), 
      dplyr::inner_join(leftDf, rightDf, by = c('db'='db', 'lda'='rda'), suffix = c('_LL', '_RR'))
    )
    
    # No need to disambiguate fields if only one side is selected
    # here `fa` is only selected in left
    df_equal(
      L$select('fa')$inner_join(R$select('db'), join_mode = 'cross_join', on_left = c('db', 'lda'), on_right = c('db', 'rda'))$to_df(), 
      dplyr::inner_join(
        leftDf %>% dplyr::select(fa, db, lda), 
        rightDf %>% dplyr::select(db, rda), 
        by = c('db'='db', 'lda'='rda')) %>%
        dplyr::select(fa)
    )
    
    # cross_join only takes dimensions as join keys
    expect_error(L$inner_join(R, join_mode = 'cross_join', on_both = c('db', 'fa')), 'fa') 
    
    # cross_join only performs inner_join
    expect_error(L$left_join(R, join_mode = 'cross_join', on_both = c('db')), 'join_mode') 
    expect_error(L$right_join(R, join_mode = 'cross_join', on_both = c('db')), 'join_mode') 
    
  }
  
  test_auto_join_keys()
  test_joins_with_conflicted_fields()
  test_cross_join_mode()
  
  L$remove_self()
  R$remove_self()
})

test_that("join with three operands", {
  df1 = data.frame(da = 1:10, db = 11:20, fa = LETTERS[1:10],  fb = 3.14 * 1:10)
  df2 = data.frame(da = 3:10, dc = 13:20, fa = LETTERS[3:10], fc = 3.14 * 3:10)
  df3 = data.frame(dd = 5:15, db = 12:22, fd = LETTERS[5:15], fe = 1.23 * 0:10)
  
  template1 = conn$array_from_schema("<fa:string, fb:double> [da;db] ")
  template2 = conn$array_from_schema("<fa:string, fc:double> [da;dc] ")
  template3 = conn$array_from_schema("<fd:string, fe:double> [dd;db] ")
  
  a1 = conn$array_from_df(df1, template1)$change_schema(template1)
  a2 = conn$array_from_df(df2, template2)$change_schema(template2)
  a3 = conn$array_from_df(df3, template3)$change_schema(template3)
  
  inner_join = dplyr::inner_join
  select = dplyr::select
  
  df_equal(
    a1$inner_join(a2, on_both = 'da')$inner_join(a3, on_both = 'db')$to_df(),
    df1 %>% inner_join(df2, by = 'da', suffix = c('_L', '_R')) %>% inner_join(df3, by = 'db')
  )
  df_equal(
    a1$select('da', 'fa', 'db')$
      inner_join(a2$select('da'), on_both = 'da')$
      inner_join(a3$select('fe'), on_both = 'db')$
      to_df(),
    df1 %>% 
      inner_join(df2 %>% select(-fa), by = 'da') %>% # remove fa from df2 other it will be suffixed with .y
      inner_join(df3, by = 'db') %>% select(da, fa, db, fe)
  )
  
})
