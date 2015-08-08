package com.github.mauricio.async.db.postgresql.messages.frontend

import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage

class PreparedStatementBindMessage(statementId: Int, query: String, values: Seq[Any], encoderRegistry : ColumnEncoderRegistry, val parseQuery : Boolean)
  extends PreparedStatementMessage(statementId, ServerMessage.Bind, query, values, encoderRegistry)