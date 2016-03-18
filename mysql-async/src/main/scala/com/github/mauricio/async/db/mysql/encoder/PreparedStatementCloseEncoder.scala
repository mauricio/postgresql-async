package com.github.mauricio.async.db.mysql.encoder
import com.github.mauricio.async.db.mysql.message.client.{ClientMessage, CloseStatementMessage}
import com.github.mauricio.async.db.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

object PreparedStatementCloseEncoder extends MessageEncoder {

  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[CloseStatementMessage]
    val buffer = ByteBufferUtils.packetBuffer(5)
    buffer.writeByte(ClientMessage.PreparedStatementClose)
    buffer.writeBytes(m.statementId)
    buffer
  }

}
