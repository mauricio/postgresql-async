package com.github.mauricio.async.db.postgresql.parsers

import com.github.mauricio.async.db.postgresql.messages.backend.{PortalSuspendedMessage, ServerMessage}
import io.netty.buffer.ByteBuf

object PortalSuspendedParser extends MessageParser {
  override def parseMessage(b: ByteBuf): ServerMessage = {
    new PortalSuspendedMessage()
  }
}

