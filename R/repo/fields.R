Field <- setRefClass("Field",
  fields = list(name = "character", type = "character", scidb_type = 'ANY'),

  methods = list(
    # Convert a scalar (length-1 vector) to a string suitable for query value
    stringify_query_value = function(scalar) {
      sprintf("%s", scalar)
    }
  )
)

as.character.Field <- function(x) {
  sprintf("%s [%s]", x$name, x$type)
}


toString.Field <- as.character.Field

NumberField <- setRefClass("NumberField",
  contains = "Field",
  methods = list(
    initialize = function(name, ...) {
      callSuper(name = name, type = "Number", ...)
    }
  )
)

TextField <- setRefClass("TextField",
  contains = "Field",
  methods = list(
    initialize = function(name, ...) {
      callSuper(name = name, type = "Text", ...)
    },

    # String value should be single quoted in query expression
    stringify_query_value = function(scalar) {
      sprintf("'%s'", scalar)
    }
  )
)



