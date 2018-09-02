package com.github.mauricio.async.db.mysql.codec

/**
  * optimize memory usage
  * we only need statementId,columnSize and parameterSize
  */
class LitePreparedStatementHolder(val statementId: Array[Byte], val columnSize: Int, val parameterSize: Int)

object LitePreparedStatementHolder {
  def apply(preparedStatementHolder: PreparedStatementHolder): LitePreparedStatementHolder = {
    val res = new LitePreparedStatementHolder(preparedStatementHolder.statementId, preparedStatementHolder.columns.size, preparedStatementHolder.parameters.size)
    res
  }
}
