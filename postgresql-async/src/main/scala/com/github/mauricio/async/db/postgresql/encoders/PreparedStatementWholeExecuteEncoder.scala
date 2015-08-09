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
import com.github.mauricio.async.db.postgresql.messages.frontend.{ClientMessage, PreparedStatementWholeExecuteMessage}
import io.netty.buffer.ByteBuf

class PreparedStatementWholeExecuteEncoder(
                                       charset: Charset,
                                       encoder : ColumnEncoderRegistry)
  extends Encoder
  with PreparedStatementEncoderHelper
{

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[PreparedStatementWholeExecuteMessage]
    val statementIdBytes = m.statementId.toString.getBytes(charset)

    writeExecutePortal( statementIdBytes, m.query, m.values, encoder, charset )
  }

}