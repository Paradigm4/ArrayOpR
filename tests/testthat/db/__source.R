context('Tests with scidb connection')

db = get_scidb_connection()
expect_true(!is.null(db), "db is not null")
