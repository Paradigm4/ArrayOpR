
# 1. ArrayOpR
An R package `arrayop` for object-oriented scidb operations/operands

<!-- TOC -->

- [1. ArrayOpR](#1-arrayopr)
    - [1.1. Installation](#11-installation)
- [2. Overview](#2-overview)
    - [2.1. Synopsis](#21-synopsis)
    - [2.2. What is ArrayOp?](#22-what-is-arrayop)
    - [2.3. ArrayOp class features](#23-arrayop-class-features)
- [3. Create a Repo instance](#3-create-a-repo-instance)
    - [3.1. Repo operations](#31-repo-operations)
    - [3.2. Prep the Repo](#32-prep-the-repo)
        - [3.2.1. Register Array Schemas](#321-register-array-schemas)
- [4. Get ArrayOp instances](#4-get-arrayop-instances)
    - [4.1. From registered array aliases](#41-from-registered-array-aliases)
    - [4.2. From array names directly](#42-from-array-names-directly)
- [5. ArrayOp instance functions](#5-arrayop-instance-functions)
    - [5.1. Lazy evaluation](#51-lazy-evaluation)
    - [5.2. Query](#52-query)
        - [5.2.1. where](#521-where)
        - [5.2.2. match](#522-match)
            - [5.2.2.1. Match by data frame with filter mode](#5221-match-by-data-frame-with-filter-mode)
            - [5.2.2.2. Match another ArrayOp with cross_between mode](#5222-match-another-arrayop-with-cross_between-mode)
        - [5.2.3. join two ArrayOps](#523-join-two-arrayops)
            - [5.2.3.1. Use 'select' to specify fields in 'join' result](#5231-use-select-to-specify-fields-in-join-result)
    - [5.3. I/O](#53-io)
        - [5.3.1. load_file](#531-load_file)
            - [5.3.1.1. Specify file headers when file fields do not match exactly the template fields](#5311-specify-file-headers-when-file-fields-do-not-match-exactly-the-template-fields)
            - [5.3.1.2. Result schema](#5312-result-schema)
            - [5.3.1.3. Customized field conversion](#5313-customized-field-conversion)
        - [5.3.2. write_to](#532-write_to)
            - [5.3.2.1. All fields content are available](#5321-all-fields-content-are-available)
            - [5.3.2.2. Auto-incremented field](#5322-auto-incremented-field)
            - [5.3.2.3. Anti-collision field](#5323-anti-collision-field)
            - [5.3.2.4. Combining auto-increment field and anti-collision field](#5324-combining-auto-increment-field-and-anti-collision-field)
        - [5.3.3. build_new](#533-build_new)
    - [5.4. Schema change](#54-schema-change)
        - [5.4.1. reshape](#541-reshape)
        - [5.5. spawn](#55-spawn)

<!-- /TOC -->

## 1.1. Installation
```{r}
devtools::install_github("Paradigm4/ArrayOpR")

# Or a development branch:
devtools::install_github("Paradigm4/ArrayOpR", ref = 'v19_v18')
```

To remove `arrayop`,
```{r}
remove.packages('arrayop')
```

# 2. Overview
## 2.1. Synopsis
  - Get `ArrayOp` instances from a `Repo`
  - Perform operations on `ArrayOp` instances
    - Most `ArrayOp` operations result in new `ArrayOp` instances (immutable)
    - Compose simple or complex operations by chaining/composing `ArrayOp`s.
  - Materialize `ArrayOp` instances in a `Repo` and expect results.
    - Or execute commands for side-effects only. 

## 2.2. What is ArrayOp?
An `ArrayOp` instance refers to any of the below:
  - scidb array
  - a query of one scidb array or multiple arrays 
  - an I/O operation in scidb
  - combination of the above

`ArrayOp` is more than a data strucutre for AFL in that:
  - field validation in R
  - convenience functions that enable users focus more on data logic rather than scidb details.

`Repo` is explained below.

## 2.3. ArrayOp class features

Regardless of how an `ArrayOp` instance `arrayOp` is created, it has the full features listed below instance functions:
  - Simple Query
    - `where`
    - `match`
  - Complex Query
    - `join`
    - `select`
  - IO
    - `build_new`
    - `load_file`
    - `write_to`
  - Schema change
    - `spawn`
    - `create_new_with_same_schema`
    - `get_schema_str`
    - `reshape`
  - Materialize
    - `query`
    - `execute`
  
  The latest `arrayop` package supports scidb v18 and v19. Version switch is made during `Repo` creation (see below for details).

  ## A working example and tests
  During `arrayop` package development, [an integration test script](R/test_db.R) that runs on both scidb v19 and v18 ensures `arraop` passes all tests in action. 
  
  ### Unit tests
  The unit tests do not depend on a scidb connection. But they shows how AFL is generated under the hood. Feel free to take a look at the [unit tests](tests/testthat/) or dive right into the [tests on ArrayOpBase class](tests/testthat/base/test_array_op_base.R).

# 3. Create a Repo instance

```{r}
repo = newRepo(default_namespace = 'test_ns', db = db)
```

The `Repo` instance is an abstraction of a scidb installation, where we perform common scidb related array operations without writing raw AFL. 
  - `default_namespace`: an existing scidb namespace.
  - `db` is a scidb connection instance which is returned by `db = scidb::connect(...)` where `...` is scidb connection params. 

## 3.1. Repo operations

There are two kinds of operations of `repo`:
  - `repo$query(what)`: pass in a query and get a result in return.
  - `repo$execute(what)`: only execute a command and do NOT expect result.

In both operations, param `what` is either a raw string or `ArrayOp` instance. In the latter case `ArrayOp$to_df_afl()` is called and an AFL string is returned and then passed to `query`/`execute`.

The advantage of using `arrayop` package compared to raw AFL is that we can compose complex queries/commands with intuitive `ArrayOp` functions.

## 3.2. Prep the Repo 
### 3.2.1. Register Array Schemas

After repo creation, we need to register exsiting arrays onto `repo` since no auto-scanning of scidb namespace is performed.

```{r}
# By default the repo namespace is used. So myArray1 refers to test_ns.A
repo$register_schema_alias_by_array_name('myArray1', 'A')

# In case of another namespace, we can specify the full array name with param is_full_name = T.
repo$register_schema_alias_by_array_name('myArray2', 'another_ns.myArray2', is_full_name = T)
```

NOTE: for performance concerns, array name is validated during registeration. 

# 4. Get ArrayOp instances

## 4.1. From registered array aliases

Once registered, `ArrayOp` instance will be created on first access and cached thereafter.
Here `array1` and `array2` are valid `ArrayOp` instances if the registered arrays do exist in scidb. Notice `array1` and `array2` exist in different namespaces. 
```{r}
array1 = repo$get_alias_schema('myArray1')
array2 = repo$get_alias_schema('myArray2')
```

## 4.2. From array names directly

In cases of one-time-use arrays, we may not want to register arrays before use. We can get `ArrayOp` instances directly from fully-qualified array names.

```{r}
array3 = repo$load_schema_from_db('test_ns.array_name')
```

# 5. ArrayOp instance functions

`ArrayOp` functions validate performed operations based on which operand's schema (attributes, dimensions, data types and dimension specs) and throw informative errors wherever possible. 

Suppose we have two arrays `A` and `B` in scidb:
```{r}
create array A <aa:string, ab:int64> [daa=0:*:0:*; dab=0:*:0:*]
create array B <ba:string, bb:int64> [dba=0:*:0:*; dbb=0:*:0:*]
```
And they are retrieved in R with `arrayop`: 
```{r}
repo$register_schema_alias_by_array_name('a', 'A')
repo$register_schema_alias_by_array_name('b', 'B')
a = repo$get_alias_schema('A')
b = repo$get_alias_schema('B')
```

## 5.1. Lazy evaluation

Unless one of the following is invoked, there is no scidb query performed:
  - `result = repo$query(any_array_op)`
  - `repo$execute(any_array_op)`

## 5.2. Query

### 5.2.1. where
Filter by array fields: 

`res = a$where(aa == 'value')`

`res = a$where(aa == 'value' && daa > 0)` is equivalent to `a$where(aa == 'value', daa > 0)`

`res = a$where(strlen(aa) == 1)`

Get the result data frame with `repo$query( a$where(aa == 'value') )` or `repo$query( res )`

Any non-literal R name is treated as array field names, and functions treated as scidb functions (eg. `strlen` function above). If we want to reference functions/symbols in R context, prefix expressions with `!!`. E.g: `res = a$where( ab %in% !!c(1,2,3))`, where `!!c(1,2,3)` is evaluated to a numeric vector in R first before get compiled to AFL. Without the `!!`, `c` is converted to a scidb function which doesn't exist. 

Existing R packages such as `dplyr` and `data.table` try to be clever in deciding context between R and the data frame, but that prohibits using R variable names shared by data frame fields.  For example, in `arrayop`, you can do:

```{r}
aa = 'value'
a$where(aa == !!aa)
```

Whereas in `data.table` and `dplyr`, `a$filter(aa == aa)` is equivelant to `a$filter(TRUE)` which returns all records. 


### 5.2.2. match

`match` operation also filters array content like `where`, but does it differently. While `where` filters arrays by a filter expression, `match` filters arrays by an existing R data frame or scidb array or array expression denoted by ArrayOp. 

#### 5.2.2.1. Match by data frame with filter mode
```{r}
df = data.frame(ab = c(1, 2))
# These two are equivalent
res1 = a$match(df, op_mode = 'filter')
res2 = a$filter(ab == 1 || ab == 2)
```

Fields are matched by names.
```{r}
df = data.frame(daa = c(1, 2))
# These two are equivalent
res1 = a$match(df, op_mode = 'filter')
res2 = a$filter(daa == 1 || dab == 2)
```

#### 5.2.2.2. Match another ArrayOp with cross_between mode

```{r}
a$match(b, op_mode = 'cross_between', field_mapping = list(daa == 'dba', dab = 'dbb'))
```
Notice `field_mapping` param is set since `a` and `b` don't have matching fields, otherwise matching fields are detected if `field_mapping` is not set. 


### 5.2.3. join two ArrayOps

```{r}
a$join(b, on_left='daa', on_right='dba')
a$join(b, on_left=c('daa', 'aa'), on_right=c('dba', 'ab')
```
Here `a` is the left and `b` is the right operand. By default, only join keys and array attributes are retained in the result. 

#### 5.2.3.1. Use 'select' to specify fields in 'join' result
Select fields on left operand:
```{r}
a$select('aa', 'ab', 'daa', 'dab')$join(b, on_left='daa', on_right='dba')
```
Select fields on both operands:
```{r}
a$select('aa', 'dab')$join(b$select('ba', 'dbb'), on_left='daa', on_right='dba')
```

NOTE: if there is `select`ed fields on at least one operand, the `join` result schema will conform to the selected fields. Otherwise, `join` result without `select`ed fields will follow the convention of scidb `equi_join` (i.e. drop dimensions, keep attributes and keep left join keys).

## 5.3. I/O

### 5.3.1. load_file

`load_file` creates a new ArrayOp from a template ArrayOp and a file path, which is normally used in another operation, e.g. `a$load_file(...)$write_to(b)`

`result = a$load_file('file_path')`

By default, file headers are assumed to match the template perfectly. In the example above, the file `file_path` should have four columns: `daa`, `dab`, `aa` and `ab`, each matches the template field data type in the exact order.

#### 5.3.1.1. Specify file headers when file fields do not match exactly the template fields
The `file_header` param specifies the expected headers in the file. Only those matching the template are loaded. Non-matching headers are irrelevant and are ignored. 

`result = a$load_file('file_path', file_headers = c('daa', 'skip', 'aa', 'ab', 'skip')`

#### 5.3.1.2. Result schema
The result schema has a collection of attributes comprised of matching template fields (dimensions and/or attributes) and doesn't have meaningful dimensions, due to the implementation of the scidb `aio_input` plugin. 

#### 5.3.1.3. Customized field conversion 

The input file can have errors or need data conversion. E.g.
`a$load_file('file_path', 
    field_conversion = list(aa = 'int64(@)+42', daa = 'dcast(@, bool(null))'))` where the `@` will be replaced by numbered attributes (a0, a1, ...) in `aio_input`. 

### 5.3.2. write_to

`a$write_to(b)` creates an ArrayOp that writes the content of the source array `a` to the target array `b`. 

The optional param `append` defaults to TRUE, applies to all below use cases, and determines whether the operator `insert` or `store` is used in AFL. 

#### 5.3.2.1. All fields content are available

`a$write_to(b)`

If the field data types of `a` match exactly those of `b`, then regardless of field names, `a` is `insert`ed or `store`d into `b` depending on the `append` param.

Otherwise, `a` is first `redimension`ed and then processed similarly to the above case **only** if all of the target `b` fields are present on `a`, which is required by the scidb `redimension` operator. 

#### 5.3.2.2. Auto-incremented field

`a$write_to(b, source_auto_increment = c(ab=0), target_auto_increment = c(bb=1))`

In cases of auto-incremented field, its value is normally not loaded from files, but calculated during the data loading. We can specify the params `source_auto_increment` and `target_auto_increment`, both named single-length integers, to instruct how the field will be auto incremented. 

The `source_auto_increment` param is normally a field from the result of `load_file`, `build_new` or `reshape` functions. Its value is normally `0`, as in scidb `unpack` and `build` operators. 

The `target_auto_increment` param is a field of the target. Its value only affects the initial data loading operation. 

#### 5.3.2.3. Anti-collision field

scidb does not allow duplicated cell coordinates, i.e. every combination of all dimensions must be unique. In cases of multiple cell coordinates are desired, an artificial dimension, namely the `anti_collision_field`, is required to circumvent the scidb restriction. The syntax is shown below:

`a$write_to(b, anti_collision_field = 'alt_id')`

#### 5.3.2.4. Combining auto-increment field and anti-collision field

`a$write_to(b, source_auto_increment = c(ab=0), target_auto_increment = c(bb=1), anti_collision_field = 'alt_id')`

**NOTE**: `ArrayOp` validates that all the combined source fields (regarless of matching fields, auto-increment field or anti-collision field, where applicable) must constitute the entirety of the target fields. 

### 5.3.3. build_new

`build_new` creates a new ArrayOp from a template ArrayOp and a data frame. The result ArrayOp's attributes consist of the template's dimensions and attributes, with an artificial build dimension.

`result = a$build_new(data.frame(da = c(1,2), aa = c('a', 'b')), artificial_field='z')`

The data frame is converted to a string literal for scidb `build` operator. All data frame column names should match the template fields, but not all template fields are required in the data frame. In the example above, the result ArrayOp has a dimension `z` and two attributes `da` and `aa`. The optional param `artifical_field`, when omitted, defaults to a random string. 

`build_new` is faster than uploading a data frame to scidb, but has limitations imposed by `shim` (reference to be added here).

## 5.4. Schema change
### 5.4.1. reshape

`reshape(select, dtypes = NULL, dim_mode = 'keep', artificial_field = .random_attr_name())`

`a$reshape` returns a new ArrayOp that has a subset of the template array `a`'s fields of which the names and/or data types can be modified. 

`a$reshape(c('aa'))` : the result ArrayOp has only one attribute `aa`, but the dimensions are the same as `a`, because the param `dim_mode` defaults to `keep`.

`a$reshape(c('aa'), dim_mode = 'drop')` : the result ArrayOp has only one attribute `aa` and no meaningful dimension unless a `artificial_field` param is provided which is used in scidb `unpack` operator to get rid of existing dimensions. 

`a$reshape(list('aa', aa_len = strlen(aa)), dtypes = list(aa_len = 'int64'))`: a named item in param `select` is treated as a new, derived field, whose data type can be provided in a `dtypes` param. 

NOTE: `reshape`d ArrayOp is useful in I/O related operations. 

E.g. we can only calculate a field after some initial operations, so the initially loaded ArrayOp can not have the same field name as the `write_to` target due to field name confliction. 

### 5.5. spawn

`spawn` is similar to `reshape` in the sense they both return a new ArrayOp with a modified schema than the template. But `spawn` differs from `reshape` in :
  - `spawn` only renames or excludes existing template fields, and does not allow new fields.
  - `spawn` result should only be used in another operation for schema template purposes but NOT for AFL in `to_afl`. 

`a$spawn(renamed = NULL, excluded = NULL, dtypes = NULL, dim_specs = NULL) `

`a$spawn(renamed = list(aa='attr1', ab='attr2'))`: result schema has the template fields `aa` and `ab` renamed to `attr` and `attr2`, respectively. 

`a$spawn(excluded = c('aa', 'daa'))`: template fields `aa` and `daa` are excluded in the result schema.  

The param `dtypes` and `dim_specs` can redefine field data types and dimension specs of the result schema.

NOTE: all the params can be provided in combination with each other. When both `renamed` and `excluded` are provided, renaming is performed prior to excluding the fields. 

