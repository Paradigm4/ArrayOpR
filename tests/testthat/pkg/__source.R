context('Package-level tests')

auth = yaml::yaml.load_file("~/.scidb_auth")

# Ensure a scidb conenction
.setup = function(){
  arrayop::connect(username = auth[["user-name"]], token = auth[["user-password"]], host = "127.0.0.1")
}

# before we connect, get_default_connection will throw an error
expect_error(get_default_connection(), "arrayop::connect")

.setup()

# after set up, there should be a valid scidb version
expect_true(nchar(get_default_connection()$scidb_version()) > 2)

for (test_file in list.files(".", "^test.+\\.R")) {
  source(test_file, local = T, chdir = T)
}
