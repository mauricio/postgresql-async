package com.github.mauricio.async.db.postgresql.messages.frontend

import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage

case class PreparedStatementCloseMessage(statementId : Int, isStatement : Boolean = true)
  extends ClientMessage( ServerMessage.CloseStatementOrPortal )
