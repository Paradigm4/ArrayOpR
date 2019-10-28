
# Utility functions
source(relative_path('utils.R'), local = TRUE)
source(relative_path('afl_utils.R'), local = TRUE)

# Dependency
source(relative_path('dependency.R'), local = TRUE)

# Base class: ArrayOpBase
source(relative_path('array_op_base.R'), local = TRUE)
# Sub-classes
source(relative_path('subset_op.R'), local = TRUE)
source(relative_path('join_op.R'), local = TRUE)
source(relative_path('array_schema.R'), local = TRUE)
source(relative_path('match_op.R'), local = TRUE)
source(relative_path('customized_op.R'), local = TRUE)
source(relative_path('write_op.R'), local = TRUE)
