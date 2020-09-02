context("ArrayOp: summarize")

# We use the R CO2 dataset
# and add three integer columns as array dimensions 

dfCO2 = datasets::CO2 %>% 
  dplyr::mutate(
    Plant_id = as.integer(Plant), 
    Type_id = as.integer(Type),
    Plant = as.character(Plant), 
    Type = as.character(Type),
    Treatment = as.character(Treatment),
    uid = 1:dplyr::n()
)

arrayCO2 = conn$upload_df(
  dfCO2,
  "<Plant:string, Type:string, Treatment:string, conc:double, uptake:double> [Plant_id; Type_id; uid]",
  force_template_schema = T
)$persist(.gc = F)

expect_df_equal = function(df1, df2) {
  nameSymbols = sapply(names(df1), as.name)
  expect_equal(
     df1 %>% dplyr::arrange(!!! nameSymbols),
     df2 %>% dplyr::arrange(!!! nameSymbols)
  )
}

test_that("group by dimensions", {
  expect_df_equal(
    arrayCO2$group_by("Plant_id")$
      summarize("max_c" = "max(conc)", "min_u" = "min(uptake)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant_id) %>% 
      dplyr::summarize(max_c = max(conc), min_u = min(uptake)) %>%
      data.frame
  )
  expect_df_equal(
    arrayCO2$group_by("Plant_id", "Type_id")$
      summarize("max_c" = "max(conc)", "min_u" = "min(uptake)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant_id, Type_id) %>% 
      dplyr::summarize(max_c = max(conc), min_u = min(uptake)) %>%
      data.frame
  )
})

test_that("group by attributes", {
  expect_df_equal(
    arrayCO2$group_by("Plant")$
      summarize("max_c" = "max(conc)", "min_u" = "min(uptake)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant) %>% 
      dplyr::summarize(max_c = max(conc), min_u = min(uptake)) %>%
      data.frame
  )
  expect_df_equal(
    arrayCO2$group_by("Plant", "Type")$
      summarize("max_c" = "max(conc)", "min_u" = "min(uptake)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant, Type) %>% 
      dplyr::summarize(max_c = max(conc), min_u = min(uptake)) %>%
      data.frame
  )
})

test_that("group by both dims and attributes", {
  expect_df_equal(
    arrayCO2$group_by("Plant", "Type_id")$
      summarize("max_c" = "max(conc)", "cnt" = "count(*)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant, Type_id) %>% 
      dplyr::summarize(max_c = max(conc), cnt = dplyr::n()) %>%
      data.frame
  )
  # aggregate on text fields
  expect_df_equal(
    arrayCO2$group_by("Plant", "Type_id")$
      summarize("max_t" = "max(Treatment)", "min_t" = "min(Treatment)")$
      to_df() 
    ,
    dfCO2 %>% 
      dplyr::group_by(Plant, Type_id) %>% 
      dplyr::summarize(max_t = max(Treatment), min_t = min(Treatment)) %>%
      data.frame
  )
})

test_that("Error cases", {
  expect_error(arrayCO2$summarize("max(conc)"), "group_by_fields")
  expect_error(arrayCO2$group_by("non-existent"), "non-existent")
})


arrayCO2$remove_self()
