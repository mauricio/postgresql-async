package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementBindMessage}
import io.netty.buffer.ByteBuf

class PreparedStatementBindEncoder(charset: Charset, encoder : ColumnEncoderRegistry) extends Encoder with PreparedStatementEncoderHelper {
  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementBindMessage]
    val statementIdBytes = m.statementId.toString.getBytes(charset)

    bind(statementIdBytes, m.query, m.values, encoder, charset)
  }
}
