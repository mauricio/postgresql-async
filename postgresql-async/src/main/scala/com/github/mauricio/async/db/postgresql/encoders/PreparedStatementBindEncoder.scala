package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementBindMessage}
import io.netty.buffer.{Unpooled, ByteBuf}

class PreparedStatementBindEncoder(charset: Charset, encoder : ColumnEncoderRegistry) extends Encoder with PreparedStatementEncoderHelper {
  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementBindMessage]
    val statementIdBytes = m.statementId.toString.getBytes(charset)

    if (m.parseQuery) {
      val parseBuffer = parse(statementIdBytes, m.query, m.valueTypes, charset)
      val bindBuffer = bind(statementIdBytes, m.query, m.values, encoder, charset, writeDescribe = m.parseQuery)
      Unpooled.wrappedBuffer(parseBuffer, bindBuffer)
    } else {
      bind(statementIdBytes, m.query, m.values, encoder, charset, writeDescribe = m.parseQuery)
    }
  }
}
