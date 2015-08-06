package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementOpeningMessage}
import io.netty.buffer.{ByteBuf, Unpooled}

class PreparedStatementCloseEncoder(charset: Charset) extends Encoder with PreparedStatementEncoderHelper {
  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementOpeningMessage]

    val statementIdBytes = m.statementId.toString.getBytes(charset)
    val closeBuffer: ByteBuf = closePortal(statementIdBytes)
    val syncBuffer: ByteBuf = sync
    Unpooled.wrappedBuffer(syncBuffer, closeBuffer)
  }
}
