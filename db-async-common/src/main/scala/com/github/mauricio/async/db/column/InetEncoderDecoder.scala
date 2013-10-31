/*
 * Copyright 2013 Maurício Linhares
 * Copyright 2013 Dylan Simon
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

package com.github.mauricio.async.db.column

import java.net.InetAddress
import com.github.mauricio.async.db.exceptions.DateEncoderNotAvailableException

object InetEncoderDecoder extends ColumnEncoderDecoder {

  override def encode(value : Any) : String = value match {
    case i : InetAddress => i.getHostAddress
    case s : String => s 
    case _ => throw new DateEncoderNotAvailableException(value)
  }

  def decode(value : String) : InetAddress =
    /* this is correct if value is a syntactially valid IP, otherwise it will do an inappropriate DNS lookup */
    InetAddress.getByName(value)

}
