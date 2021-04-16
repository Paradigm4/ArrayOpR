


# Install ArrayOpR package

Run the script below in your R session.
```r
devtools::install_github("Paradigm4/ArrayOpR")
# devtools::install_github("Paradigm4/ArrayOpR", ref = "nightly") # or a differnt git head other than master
```

Use arrayop 

```r
library(arrayop)

auth = yaml::yaml.load_file("~/.scidb_auth")
# Then connect to scidb. Params host, username and token are all single strings and can be provided in any way
conn = db_connect(host = "localhost", username = auth[["user-name"]], token = auth[["user-password"]])
```

To get the db connection, call `get_default_connection`, 
which returns a global connection object under the `arrayop` package namespace.

```r
conn = get_default_connection()

# If connection is timed out. 
# This only works if you save the credentails in the connection, which should only be done in testing mode.
conn$connect() # use the same credential to re-connect
```
