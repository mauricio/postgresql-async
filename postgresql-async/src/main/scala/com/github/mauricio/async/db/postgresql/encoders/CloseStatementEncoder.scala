package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage
import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementCloseMessage}
import com.github.mauricio.async.db.util.{ByteBufferUtils, Log}
import io.netty.buffer.{ByteBuf, Unpooled}

object CloseStatementEncoder {
  val log = Log.get[CloseStatementEncoder]
}

class CloseStatementEncoder(charset: Charset) extends Encoder with PreparedStatementEncoderHelper {

  import CloseStatementEncoder._

  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementCloseMessage]

    if ( log.isDebugEnabled ) {
      log.debug("Closing statement ({})", m)
    }

    closeAndSyncBuffer(m.statementId.toString.getBytes(charset), PreparedStatementEncoderHelper.Statement)
  }
}
