package com.github.mauricio.async.db.postgresql.messages.frontend

import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage

class PreparedStatementExecuteMessage(val statementId: Int, val fetchSize : Int) extends ClientMessage(ServerMessage.Execute)
