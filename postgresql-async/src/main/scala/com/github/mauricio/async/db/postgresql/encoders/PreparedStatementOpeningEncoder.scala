/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mauricio.async.db.postgresql.encoders

import java.nio.charset.Charset

import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementOpeningMessage}
import com.github.mauricio.async.db.util.Log
import io.netty.buffer.{ByteBuf, Unpooled}

object PreparedStatementOpeningEncoder {
  val log = Log.get[PreparedStatementOpeningEncoder]
}

class PreparedStatementOpeningEncoder(charset: Charset, encoder : ColumnEncoderRegistry)
  extends Encoder
  with PreparedStatementEncoderHelper
{

  override def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[PreparedStatementOpeningMessage]

    val statementIdBytes = m.statementId.toString.getBytes(charset)
    val parseBuffer: ByteBuf = parse(statementIdBytes, m.query, m.valueTypes, charset)
    val executeBuffer = writeExecutePortal(statementIdBytes, m.query, m.values, encoder, charset, writeDescribe = true)

    Unpooled.wrappedBuffer(parseBuffer, executeBuffer)
  }
}