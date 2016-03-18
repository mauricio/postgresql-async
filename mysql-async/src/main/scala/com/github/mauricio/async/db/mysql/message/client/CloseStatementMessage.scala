package com.github.mauricio.async.db.mysql.message.client

case class CloseStatementMessage( statementId : Array[Byte] )
  extends ClientMessage( ClientMessage.PreparedStatementClose )
