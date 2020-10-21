
# arrayop 1.2

## Major changes
  * Add [`ArrayOpBase$delete_cells`](https://paradigm4.github.io/ArrayOpR/reference/ArrayOpBase.html#method-delete_cells)
  * Add [`ScidbConnection$execute_mquery`](https://paradigm4.github.io/ArrayOpR/reference/ScidbConnection.html#method-execute_mquery)

# arrayop 1.1

## Major changes

  * Add [`ArrayOpBase$full_join`](https://paradigm4.github.io/ArrayOpR/reference/ArrayOpBase.html#method-full-join-)
  * Change `join` parameters from `on_left, on_right, on_both` to `by.x, by.y, by`, consistent with R `base::merge` function
  * Change [`ArrayOpBase$persist`](https://paradigm4.github.io/ArrayOpR/reference/ArrayOpBase.html#method-persist-) implementation to avoid confusion of API usage
  
## Minor changes

  * Rename version related API
    - [`ArrayOpBase$list_versions`](https://paradigm4.github.io/ArrayOpR/reference/ArrayOpBase.html#method-list-versions-) (was `versions`)
    - [`ArrayOpBase$version`](https://paradigm4.github.io/ArrayOpR/reference/ArrayOpBase.html#method-version-) (was `get_version_snapshot`)


# arrayop 1.0

First public release of the `arrayop` package
