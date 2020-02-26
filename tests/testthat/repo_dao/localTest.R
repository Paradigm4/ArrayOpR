mockDep = list(
  get_scidb_version = function()
    "19.3"
  , query = function(what, ...) list('query', what, ...)
  , execute = function(what, ...) list('execute', what, ...)
  , get_schema_df = function(...) stop("get_schema_df not implemented in the mockDep object")
)
repo = newRepo('unittest', dependency_obj = mockDep)
dao = newRepoDao(repo, db = NULL)


V = dao$get_array("unittest.V <ref:string,alt:string,rsid:string,vep_gene:string COMPRESSION 'zlib'> [chrom=1:24:0:1; pos=1:*:0:1000000; alt_id=0:*:0:100]")

ds = V$build_new(data.frame(ref = c('A', 'A'), rsid = c('rs35597240', 'rs9661808'), vep_gene = c('gene11', 'gene22'), alt = 'MUTATED',
                            stringsAsFactors = F))

updateOp = V$update_by(
  V$mutate(ds, keys = c('ref', 'rsid'), updated_fields = c('vep_gene', 'alt'))
)

print(updateOp$to_afl())

print(V$update_by(
  V$where(pos <= 62002484)$mutate(list(vep_gene = "'mutated by list expression'", rsid = "iif(alt='G', 'GGGG', 'NOT ggggg')"))
)$to_afl())

print(V$update_by(
  V$where(pos <= 62002484)$mutate(list(vep_gene = "string(null)"))
)$to_afl())


# Try with matching dimensions
ds = V$build_new(data.frame(chrom=1L, pos = c(62002418, 62002484, 62007946), alt_id = 0, vep_gene = c('A', 'B', 'C'),
                            stringsAsFactors = F))

print(V$update_by(
    V$mutate(ds, keys = V$dims, updated_fields = 'vep_gene')
  )$to_afl()
)

# ignore unmatched records
ds = V$build_new(data.frame(chrom=1L, pos = c(62002418, 62002484, 62007946), alt_id = 1, vep_gene = c('A', 'B', 'C'),
                            stringsAsFactors = F))

print(V$update_by(
    V$mutate(ds, keys = V$dims, updated_fields = 'vep_gene')
  )$to_afl()
)

# potential ambiguities with missing dimension
ds = V$build_new(data.frame(chrom=1L, pos = c(62002418, 62002484, 62007946), vep_gene = c('A', 'B', 'C'), rsid = 'CHanged',
                            stringsAsFactors = F))

print(V$update_by(
    V$mutate(ds, keys = c('pos', 'chrom'), updated_fields = c('vep_gene', 'rsid'))
  )$to_afl()
)

# potential ambiguities with missing dimension and duplicated data source records
ds = V$build_new(data.frame(chrom=1L, pos = c(62002418, 62002484, 62007946, 62002418), vep_gene = c('A', 'B', 'C', 'AA'), rsid = 'CHanged',
                            stringsAsFactors = F))
# without 'grouped_aggregate' to get unique records, 'mutate' will fail due to cell collision in 'redimension'
ds = ds$create_new_with_same_schema(afl(ds %grouped_aggregate% c('max(vep_gene) as vep_gene', 'max(rsid) as rsid', 'chrom', 'pos')))

print(V$update_by(
    V$mutate(ds, keys = c('pos', 'chrom'), updated_fields = c('vep_gene', 'rsid'))
  )$to_afl()
)


# potential ambiguities with missing dimension and duplicated data source records
ds = V$build_new(data.frame(chrom=1L, pos = c(62002418, 62002484, 62007946, 62002418), vep_gene = c('A', 'B', 'C', 'AA'), rsid = 'CHanged',
                            stringsAsFactors = F))
# Here we use a redimension setting to ignore cell collision 
print(V$update_by(
    V$mutate(ds, keys = c('pos', 'chrom'), updated_fields = c('vep_gene', 'rsid'), 
              # Resolve cell collision by a customized redimension setting isStrict = false
             .redimension_setting = c('false', 'cells_per_chunk: 1234'))
  )$to_afl()
)
