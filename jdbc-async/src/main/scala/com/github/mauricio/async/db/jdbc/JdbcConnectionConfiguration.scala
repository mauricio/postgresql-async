package com.github.mauricio.async.db.jdbc

/**
  * A configuration of JdbcConnection.
  *
  * @param jdbcUrl     a JDBC url
  * @param driverProps JDBC driver properties
  */
case class JdbcConnectionConfiguration(
                                        jdbcUrl: String,
                                        driverProps: Map[String, String]
                                      )