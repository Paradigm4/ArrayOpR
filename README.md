# ArrayOpR
R package for object-oriented scidb operations/operands

## Installation
```{r}
devtools::install_github("Paradigm4/ArrayOpR", ref = 'v19_v18')
```

To remove `arrayop`,
```{r}
remove.packages('arrayop')
```

## Note
  - Currently only sicdb v18.1 is targeted. New syntax changes in scidb v19 may break something. 
  Future releases will address the need of different scidb versions/connections in one application.
  - There are unit tests that demonstrate usage of `arrayop` package. Comprehensive documentation is still in progress.
  Tests can be found at `pkg_root/tests/testthat/`
  - Pull requests and feedback are greatly appreciated.
  
