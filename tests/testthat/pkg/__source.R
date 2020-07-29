context('Package-level tests')

auth = yaml::yaml.load_file("~/.scidb_auth")

# Ensure a scidb conenction
.setup = function(){
  arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], host = "127.0.0.1")
}

# before we connect, scidb_version() should be NULL
expect_null(get_default_connection()$scidb_version())

.setup()

for (test_file in list.files(".", "^test.+\\.R")) {
  source(test_file, local = T, chdir = T)
}
