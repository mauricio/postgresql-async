package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementExecuteMessage}
import io.netty.buffer.ByteBuf

class PreparedStatementExecuteEncoder(charset: Charset) extends Encoder with PreparedStatementEncoderHelper {
  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementExecuteMessage]

    val statementIdBytes = m.statementId.toString.getBytes(charset)
    execute(statementIdBytes, m.fetchSize)
  }
}