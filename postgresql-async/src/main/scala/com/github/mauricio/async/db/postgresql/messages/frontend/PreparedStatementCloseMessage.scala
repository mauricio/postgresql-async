package com.github.mauricio.async.db.postgresql.messages.frontend

import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage

class PreparedStatementCloseMessage(val statementId: Int) extends ClientMessage(ServerMessage.CloseStatementOrPortal)
