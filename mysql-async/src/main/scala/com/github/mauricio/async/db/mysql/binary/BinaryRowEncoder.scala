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

package com.github.mauricio.async.db.mysql.binary

import java.nio.charset.Charset
import com.github.mauricio.async.db.mysql.binary.encoder._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.github.mauricio.async.db.util._
import org.joda.time._
import scala.Some

object BinaryRowEncoder {
  final val log = Log.get[BinaryRowEncoder]
}

class BinaryRowEncoder( charset : Charset ) {

  import BinaryRowEncoder.log

  private final val stringEncoder = new StringEncoder(charset)
  private final val encoders = Map[Class[_],BinaryEncoder](
    classOf[String] -> this.stringEncoder,
    classOf[BigInt] -> this.stringEncoder,
    classOf[BigDecimal] -> this.stringEncoder,
    classOf[java.math.BigDecimal] -> this.stringEncoder,
    classOf[java.math.BigInteger] -> this.stringEncoder,
    classOf[Byte] -> ByteEncoder,
    classOf[java.lang.Byte] -> ByteEncoder,
    classOf[Short] -> ShortEncoder,
    classOf[java.lang.Short] -> ShortEncoder,
    classOf[Int] -> IntegerEncoder,
    classOf[java.lang.Integer] -> IntegerEncoder,
    classOf[Long] -> LongEncoder,
    classOf[java.lang.Long] -> LongEncoder,
    classOf[Float] -> FloatEncoder,
    classOf[java.lang.Float] -> FloatEncoder,
    classOf[Double] -> DoubleEncoder,
    classOf[java.lang.Double] -> DoubleEncoder,
    classOf[LocalDateTime] -> LocalDateTimeEncoder,
    classOf[DateTime] -> DateTimeEncoder,
    classOf[LocalDate] -> LocalDateEncoder,
    classOf[java.util.Date] -> JavaDateEncoder,
    classOf[java.sql.Timestamp] -> SQLTimestampEncoder,
    classOf[java.sql.Date] -> SQLDateEncoder,
    classOf[java.sql.Time] -> SQLTimeEncoder,
    classOf[scala.concurrent.duration.FiniteDuration] -> DurationEncoder,
    classOf[Array[Byte]] -> ByteArrayEncoder,
    classOf[Boolean] -> BooleanEncoder,
    classOf[java.lang.Boolean] -> BooleanEncoder
  )

  def encode( values : Seq[Any] ) : ChannelBuffer = {

    val bitMapBuffer = ChannelUtils.mysqlBuffer(values.length)
    val parameterTypesBuffer = ChannelUtils.mysqlBuffer(values.size * 2)
    val parameterValuesBuffer = ChannelUtils.mysqlBuffer()
    val bitMap = new BitMap( new Array[Byte]( (values.size + 7) / 8 ) )

    var index = 0
    var allNulls = true

    while ( index < values.length ) {
      val value = values(index)
      if ( value == null ) {
        bitMap.set(index)
      } else {
        allNulls = false
        val encoder = encoderFor(value)
        parameterTypesBuffer.writeShort(encoder.encodesTo)
        encoder.encode(value, parameterValuesBuffer)
      }
      index += 1
    }

    bitMap.write(bitMapBuffer)
    if ( allNulls ) {
      bitMapBuffer.writeByte(0)
    } else {
      bitMapBuffer.writeByte(1)
    }

    ChannelBuffers.wrappedBuffer( bitMapBuffer, parameterTypesBuffer, parameterValuesBuffer )
  }

  private def encoderFor( v : Any ) : BinaryEncoder = {

    this.encoders.get(v.getClass) match {
      case Some(encoder) => encoder
      case None => {
        v match {
          case v : CharSequence => this.stringEncoder
          case v : BigInt => this.stringEncoder
          case v : java.math.BigInteger => this.stringEncoder
          case v : BigDecimal => this.stringEncoder
          case v : java.math.BigDecimal => this.stringEncoder
          case v : ReadableDateTime => DateTimeEncoder
          case v : ReadableInstant => ReadableInstantEncoder
          case v : LocalDateTime => LocalDateTimeEncoder
          case v : java.sql.Timestamp => SQLTimestampEncoder
          case v : java.sql.Date => SQLDateEncoder
          case v : java.util.Calendar => CalendarEncoder
          case v : LocalDate => LocalDateEncoder
          case v : LocalTime => LocalTimeEncoder
          case v : java.sql.Time => SQLTimeEncoder
          case v : scala.concurrent.duration.Duration => DurationEncoder
          case v : java.util.Date => JavaDateEncoder
        }
      }
    }

  }

}
