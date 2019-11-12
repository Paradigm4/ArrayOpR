context('old array op tests')

.old_func = DEP$df_to_arrayop_func
# Set up shared code for all tests
source(relative_path('setup.R'), local = TRUE)

source(relative_path('test-afl-util.R'), local = TRUE)

source(relative_path('test-array-op-base.R'), local = TRUE)
source(relative_path('test-array-op-base-convenience-methods.R'), local = TRUE)
source(relative_path('test-subset-op.R'), local = TRUE)
source(relative_path('test-join-op.R'), local = TRUE)
source(relative_path('test-array-schema.R'), local = TRUE)
source(relative_path('test-match-op.R'), local = TRUE)
source(relative_path('test-customized-op.R'), local = TRUE)
source(relative_path('test-write-op.R'), local = TRUE)

DEP$df_to_arrayop_func = .old_func
