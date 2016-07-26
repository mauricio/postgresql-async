package com.github.mauricio.async.db.jdbc

import java.sql.ResultSetMetaData

import com.github.mauricio.async.db.general.ColumnData

object JdbcColumnData {
  def apply(md: ResultSetMetaData, col: Int): JdbcColumnData = {
    JdbcColumnData(
      name = md.getColumnName(col),
      dataType = md.getColumnType(col),
      dataTypeSize = md.getPrecision(col)
    )
  }
}

case class JdbcColumnData(name: String,
                          dataType: Int,
                          dataTypeSize: Long) extends ColumnData