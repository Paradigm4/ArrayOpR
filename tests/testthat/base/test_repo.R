context("Test Repo class")


# RepoBase class --------------------------------------------------------------------------------------------------
# Mock iquery(..., return = T) with a query that returns repeated afl x times
dummy_query = function(afl, times = 2, ...) {
  paste(rep(afl, times), collapse = '')
}
# Mock iquery(..., return = F)
dummy_execute = function(afl, ....) {
  sprintf("Cmd: %s", afl)
}

mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = dummy_query
  , execute = dummy_execute
  , get_schema_df = identity
)

mockDep2 = list(
  get_scidb_version = function()
    "18.1"
  , query = dummy_query
  , execute = dummy_execute
  , get_schema_df = identity
)

test_that("Version switch", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$meta[['scidb_version']], '19.3')
  expect_identical(repo$meta[['repo_version']], 'RepoV19')
  
  repo2 = newRepo(default_namespace = 'ns', dependency_obj = mockDep2)
  expect_identical(repo2$meta[['scidb_version']], '18.1')
  expect_identical(repo2$meta[['repo_version']], 'RepoV18')
})

test_that("DB dependency delegation", {
  repo = newRepo(default_namespace = 'ns', dependency_obj = mockDep)
  expect_identical(repo$query('afl'), 'aflafl')
  expect_identical(repo$query('afl', times = 3), 'aflaflafl')
  expect_identical(repo$execute('afl'), 'Cmd: afl')
})

test_that("No matched version", {
  dep = list(get_scidb_version = function() '20.2', query = identity, execute = identity, get_schema_df = identity)
  expect_error(newRepo('ns', dependency_obj = dep), 'unsupported scidb version')
})


# Schema management -----------------------------------------------------------------------------------------------

test_that("Reigstered schemas are stored in schema registry", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = identity)
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name('a', 'A')
  repo$register_schema_alias_by_array_name('b', 'public.B', is_full_name = T)
  expect_identical(repo$meta$schema_registry, list(a = 'ns.A', b = 'public.B'))
  
  # Register multiple schemas at once
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name(alias = c('a', 'b'), array_name = c('A', 'B'))
  expect_identical(repo$meta$schema_registry, list(a = 'ns.A', b = 'ns.B'))
})

test_that("Create ArrayOp instance for registered array schema aliases", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = function(x) data.frame(name = 'a', dtype = 'string', is_dimension = F))
  repo = newRepo('ns', dep)
  repo$register_schema_alias_by_array_name('a', 'A')
  schema = repo$get_alias_schema('a')
  expect_identical(schema$to_afl(), 'ns.A')
  expect_error(repo$get_alias_schema('non-existent'), 'not registered')
})

# Load repo from a setting object (list) ----------

test_that("Load repo schemas from a setting object", {
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
    get_schema_df = identity)
  config = list(namespace = 'ns', settings = list(), 
    arrays = list(
      list(alias = 'V', name = 'Variant', schema = "<ref:string, alt:string compression 'zlib', score:double> [chrom=1:24:0:1; pos=*]"),
      list(alias = 'G', name = 'Genotype', 
        attrs = list(ref='string', alt='string', score='double'), 
        dims = list(chrom='1:24:0:1', pos='*'))
    ))
  yamlStr = "# comment line
namespace: ns
arrays:
  - alias: V
    name: Variant
    schema: >
      <ref:string, alt:string compression 'zlib', score:double> 
      [chrom=1:24:0:1; pos=*]
      
  - alias: G
    name: Genotype
    dims:
      chrom: '1:24:0:1'
      # a single * has to be quoted otherwise will cause a yaml parse error
      pos: '*'
    attrs:
      ref: string
      alt: string
      score: double
"
  for(repo in c(
    newRepo(dep = dep, config = config)
    , newRepo(dep = dep, config = yaml::yaml.load(yamlStr))
  )){
    V = repo$get_alias_schema('V')
    G = repo$get_alias_schema('G')
    expect_identical(V$to_afl(), 'ns.Variant')
    expect_identical(V$dims, c('chrom', 'pos'))
    expect_identical(V$attrs, c('ref', 'alt', 'score'))
    assert_afl_equal(V$to_schema_str(), "<ref:string, alt:string compression 'zlib', score:double> [chrom=1:24:0:1; pos=*]")
    
    expect_identical(G$to_afl(), 'ns.Genotype')
    expect_identical(G$dims, c('chrom', 'pos'))
    expect_identical(G$attrs, c('ref', 'alt', 'score'))
    assert_afl_equal(G$to_schema_str(), "<ref:string, alt:string, score:double> [chrom=1:24:0:1; pos=*]")
  }
})

test_that("Load repo from a more complicated repo setting", {
  yamlStr = "
namespace: VWTV1
arrays:
  - alias: DataVersion
    name: VARIANT_WAREHOUSE_META.DATA_VERSION
    schema: \"<name:string,description:string,namespace:string,created_on:datetime,updated_on:datetime,created_by:string> [data_version_id=0:*:0:100000]\"

  - alias: DataSource
    name: VARIANT_WAREHOUSE_META.SAMPLE_DATA_SOURCE
    schema: \"<data_version_id:int64,name:string,description:string,created_on:datetime,updated_on:datetime,sample_count:int64,sample_fields_count:int64,array_name:string> [sample_data_source_id=0:*:0:100000]\"

  # Arrays for all sample data sources
  #
  - alias: sample_key_id_map
    name: SAMPLE_KEY_ID_MAP
    schema: '<sample_key: string> [sample_id=0:*:0:100000, data_source_id=0:*:0:10] '

  # Individual Sample arrays
  - alias: sample_clinical
    name: SAMPLE_CLINICAL
    schema: >
      <tumor_normal_ID: string,unique_ID:string,BMSPROJECTID:string,trial_ID:string,trial_indication:string,PN:string,tumor_ID:string,normal_ID:string,cohort_pair_value:double,best_cohort_match_ID:string,best_cohort_match_score:double,tumor_seq_pass_fail:string,normal_seq_pass_fail:string,cohort_pass_fail:string,gender:string,type:string,QC_pass_fail:string,TMB_missense:uint16,TMB_strelka_indels:double,TMB_strelka_frameshifts:uint16,TMB_tnscope_indels:uint16,TMB_tnscope_frameshifts:uint16,TP_strelka:double,TP_Tnsnv:double,TP_VarDict:double,TH_strelka_cluster_count:uint8,TH_Tnsnv_cluster_count:uint8,TH_VarDict_cluster_count:uint8,TH_strelka_tumor_evolution:string,TH_Tnsnv_tumor_evolution:string,TH_strelka_cluster1_mean:double,TH_strelka_cluster1_number_of_mut:uint16,TH_strelka_cluster2_mean:double,TH_strelka_cluster2_number_of_mut:uint16,TH_strelka_cluster3_mean:double,TH_strelka_cluster3_number_of_mut:uint16,TH_strelka_cluster4_mean:double,TH_strelka_cluster4_number_of_mut:uint16,TH_strelka_cluster5_mean:double,TH_strelka_cluster5_number_of_mut:uint16,TH_Tnsnv_cluster1_mean:double,TH_Tnsnv_cluster1_number_of_mut:uint16,TH_Tnsnv_cluster2_mean:double,TH_Tnsnv_cluster2_number_of_mut:double,TH_Tnsnv_cluster3_mean:double,TH_Tnsnv_cluster3_number_of_mut:double,TH_Tnsnv_cluster4_mean:double,TH_Tnsnv_cluster4_number_of_mut:double,TH_Tnsnv_cluster5_mean:double,TH_Tnsnv_cluster5_number_of_mut:double,TH_VarDict_cluster1_mean:double,TH_VarDict_cluster1_number_of_mut:uint16,TH_VarDict_cluster2_mean:double,TH_VarDict_cluster2_number_of_mut:uint16,TH_VarDict_cluster3_mean:double,TH_VarDict_cluster3_number_of_mut:uint16,TH_VarDict_cluster4_mean:double,TH_VarDict_cluster4_number_of_mut:uint16,TH_VarDict_cluster5_mean:double,TH_VarDict_cluster5_number_of_mut:uint16,Top_MutationSignature:uint8,MutationSignature1:double,MutationSignature2:double,MutationSignature3:double,MutationSignature4:double,MutationSignature5:double,MutationSignature6:double,MutationSignature7:double,MutationSignature8:double,MutationSignature9:double,MutationSignature10:double,MutationSignature11:double,MutationSignature12:double,MutationSignature13:double,MutationSignature14:double,MutationSignature15:double,MutationSignature16:double,MutationSignature17:double,MutationSignature18:double,MutationSignature19:double,MutationSignature20:double,MutationSignature21:double,MutationSignature22:double,MutationSignature23:double,MutationSignature24:double,MutationSignature25:double,MutationSignature26:double,MutationSignature27:double,MutationSignature28:double,MutationSignature29:double,MutationSignature30:double,MSI_Total_Number_of_Sites:uint16,MSI_Number_of_Somatic_Sites:uint16,MSI_percent:double,tumor_HLA_A1:string,tumor_HLA_A2:string,tumor_HLA_B1:string,tumor_HLA_B2:string,tumor_HLA_C1:string,tumor_HLA_C2:string,normal_HLA_A1:string,normal_HLA_A2:string,normal_HLA_B1:string,normal_HLA_B2:string,normal_HLA_C1:string,normal_HLA_C2:string,neoantigens_total_number:uint32,neoantigens_number_with_threshold_0_8:uint32,neoantigens_SNV:uint32,neoantigens_SNV_with_threshold_0_8:uint32,neoantigens_indels:uint32,neoantigens_indels_with_threshold_0_8:uint32,manifest_tumor_VSLABL:string,manifest_tumor_VISITD:string,manifest_tumor_SEX:string,manifest_tumor_ACCNUM:string,manifest_tumor_BARCODE:string,manifest_tumor_BARCODESRC:string,manifest_tumor_SPECTYPE:string,manifest_tumor_POOLID:string,manifest_tumor_BATCHID:string,manifest_tumor_WELLID:string,manifest_tumor_RUNDATE:string,manifest_tumor_QCFLAG:string,manifest_tumor_VHYB:string,manifest_tumor_VLANE:string,manifest_tumor_VBARCODE:string,manifest_normal_VSLABL:string,manifest_normal_VISITD:string,manifest_normal_SEX:string,manifest_normal_ACCNUM:string,manifest_normal_BARCODE:string,manifest_normal_BARCODESRC:string,manifest_normal_SPECTYPE:string,manifest_normal_POOLID:string,manifest_normal_BATCHID:string,manifest_normal_WELLID:string,manifest_normal_RUNDATE:string,manifest_normal_QCFLAG:string,manifest_normal_VHYB:string,manifest_normal_VLANE:string,manifest_normal_VBARCODE:string,tumor_Metrics_BAIT_SET:string,tumor_Metrics_GENOME_SIZE:double,tumor_Metrics_BAIT_TERRITORY:uint32,tumor_Metrics_TARGET_TERRITORY:uint32,tumor_Metrics_BAIT_DESIGN_EFFICIENCY:double,tumor_Metrics_TOTAL_READS:uint32,tumor_Metrics_PF_READS:uint32,tumor_Metrics_PF_UNIQUE_READS:uint32,tumor_Metrics_PCT_PF_READS:uint8,tumor_Metrics_PCT_PF_UQ_READS:double,tumor_Metrics_PF_UQ_READS_ALIGNED:uint32,tumor_Metrics_PCT_PF_UQ_READS_ALIGNED:double,tumor_Metrics_PF_BASES_ALIGNED:double,tumor_Metrics_PF_UQ_BASES_ALIGNED:double,tumor_Metrics_ON_BAIT_BASES:double,tumor_Metrics_NEAR_BAIT_BASES:double,tumor_Metrics_OFF_BAIT_BASES:double,tumor_Metrics_ON_TARGET_BASES:double,tumor_Metrics_PCT_SELECTED_BASES:double,tumor_Metrics_PCT_OFF_BAIT:double,tumor_Metrics_ON_BAIT_VS_SELECTED:double,tumor_Metrics_MEAN_BAIT_COVERAGE:double,tumor_Metrics_MEAN_TARGET_COVERAGE:double,tumor_Metrics_MEDIAN_TARGET_COVERAGE:uint16,tumor_Metrics_PCT_USABLE_BASES_ON_BAIT:double,tumor_Metrics_PCT_USABLE_BASES_ON_TARGET:double,tumor_Metrics_FOLD_ENRICHMENT:double,tumor_Metrics_ZERO_CVG_TARGETS_PCT:double,tumor_Metrics_PCT_EXC_DUPE:double,tumor_Metrics_PCT_EXC_MAPQ:double,tumor_Metrics_PCT_EXC_BASEQ:double,tumor_Metrics_PCT_EXC_OVERLAP:double,tumor_Metrics_PCT_EXC_OFF_TARGET:double,tumor_Metrics_FOLD_80_BASE_PENALTY:string,tumor_Metrics_PCT_TARGET_BASES_1X:double,tumor_Metrics_PCT_TARGET_BASES_2X:double,tumor_Metrics_PCT_TARGET_BASES_10X:double,tumor_Metrics_PCT_TARGET_BASES_20X:double,tumor_Metrics_PCT_TARGET_BASES_30X:double,tumor_Metrics_PCT_TARGET_BASES_40X:double,tumor_Metrics_PCT_TARGET_BASES_50X:double,tumor_Metrics_PCT_TARGET_BASES_100X:double,tumor_Metrics_HS_LIBRARY_SIZE:uint32,tumor_Metrics_HS_PENALTY_10X:double,tumor_Metrics_HS_PENALTY_20X:double,tumor_Metrics_HS_PENALTY_30X:double,tumor_Metrics_HS_PENALTY_40X:double,tumor_Metrics_HS_PENALTY_50X:double,tumor_Metrics_HS_PENALTY_100X:double,tumor_Metrics_AT_DROPOUT:double,tumor_Metrics_GC_DROPOUT:double,tumor_Metrics_HET_SNP_SENSITIVITY:double,tumor_Metrics_HET_SNP_Q:uint8,tumor_Metrics_SAMPLE:string,tumor_Metrics_LIBRARY:string,tumor_Metrics_READ_GROUP:string,normal_Metrics_BAIT_SET:string,normal_Metrics_GENOME_SIZE:double,normal_Metrics_BAIT_TERRITORY:uint32,normal_Metrics_TARGET_TERRITORY:uint32,normal_Metrics_BAIT_DESIGN_EFFICIENCY:double,normal_Metrics_TOTAL_READS:uint32,normal_Metrics_PF_READS:uint32,normal_Metrics_PF_UNIQUE_READS:uint32,normal_Metrics_PCT_PF_READS:uint8,normal_Metrics_PCT_PF_UQ_READS:double,normal_Metrics_PF_UQ_READS_ALIGNED:uint32,normal_Metrics_PCT_PF_UQ_READS_ALIGNED:double,normal_Metrics_PF_BASES_ALIGNED:double,normal_Metrics_PF_UQ_BASES_ALIGNED:double,normal_Metrics_ON_BAIT_BASES:double,normal_Metrics_NEAR_BAIT_BASES:double,normal_Metrics_OFF_BAIT_BASES:double,normal_Metrics_ON_TARGET_BASES:double,normal_Metrics_PCT_SELECTED_BASES:double,normal_Metrics_PCT_OFF_BAIT:double,normal_Metrics_ON_BAIT_VS_SELECTED:double,normal_Metrics_MEAN_BAIT_COVERAGE:double,normal_Metrics_MEAN_TARGET_COVERAGE:double,normal_Metrics_MEDIAN_TARGET_COVERAGE:uint16,normal_Metrics_PCT_USABLE_BASES_ON_BAIT:double,normal_Metrics_PCT_USABLE_BASES_ON_TARGET:double,normal_Metrics_FOLD_ENRICHMENT:double,normal_Metrics_ZERO_CVG_TARGETS_PCT:double,normal_Metrics_PCT_EXC_DUPE:double,normal_Metrics_PCT_EXC_MAPQ:double,normal_Metrics_PCT_EXC_BASEQ:double,normal_Metrics_PCT_EXC_OVERLAP:double,normal_Metrics_PCT_EXC_OFF_TARGET:double,normal_Metrics_FOLD_80_BASE_PENALTY:string,normal_Metrics_PCT_TARGET_BASES_1X:double,normal_Metrics_PCT_TARGET_BASES_2X:double,normal_Metrics_PCT_TARGET_BASES_10X:double,normal_Metrics_PCT_TARGET_BASES_20X:double,normal_Metrics_PCT_TARGET_BASES_30X:double,normal_Metrics_PCT_TARGET_BASES_40X:double,normal_Metrics_PCT_TARGET_BASES_50X:double,normal_Metrics_PCT_TARGET_BASES_100X:double,normal_Metrics_HS_LIBRARY_SIZE:uint32,normal_Metrics_HS_PENALTY_10X:double,normal_Metrics_HS_PENALTY_20X:double,normal_Metrics_HS_PENALTY_30X:double,normal_Metrics_HS_PENALTY_40X:double,normal_Metrics_HS_PENALTY_50X:double,normal_Metrics_HS_PENALTY_100X:double,normal_Metrics_AT_DROPOUT:double,normal_Metrics_GC_DROPOUT:double,normal_Metrics_HET_SNP_SENSITIVITY:double,normal_Metrics_HET_SNP_Q:uint8,normal_Metrics_SAMPLE:string,normal_Metrics_LIBRARY:string,normal_Metrics_READ_GROUP:string,problem_JIRA:string,clinical_site_ID:uint16,EA_name:string>
      [sample_record_id=0:*:0:100000; sample_id=0:*:0:100000]
  "
  dep = list(get_scidb_version = function() '18.1', query = function(x) 42, execute = function(x) 'cmd', 
             get_schema_df = identity)
  repo = newRepo(config = yaml::yaml.load(yamlStr), dep = dep)
  expect_identical(repo, repo)
})
