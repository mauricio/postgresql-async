package com.github.mauricio.async.db.postgresql.column

import com.github.mauricio.async.db.postgresql.messages.backend.PostgreSQLColumnData
import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil
import org.joda.time
import org.joda.time.DateTimeZone
import org.specs2.mutable.Specification

class PostgreSQLTimestampEncoderDecoderSpec extends Specification {

  def execute( columnType:Int, data : String ) : Any = {
    val date = data.getBytes( CharsetUtil.UTF_8 )
    PostgreSQLTimestampEncoderDecoder.decode(
      PostgreSQLColumnData("name",1,1,columnType,1,1,1),
      Unpooled.wrappedBuffer(date), CharsetUtil.UTF_8)
  }

  "encoder/decoder" should {

    "parse a date" in {
      val localDateTime = execute(ColumnTypes.Timestamp, "2018-08-15 18:26:36").asInstanceOf[time.LocalDateTime]
      localDateTime.getYear === 2018
      localDateTime.getMonthOfYear === 8
      localDateTime.getDayOfMonth === 15
      localDateTime.getHourOfDay === 18
      localDateTime.getMinuteOfHour === 26
      localDateTime.getSecondOfMinute === 36

    }

    "parse a date with timezone" in {
      DateTimeZone.setDefault(DateTimeZone.UTC)
      val dateTime = execute(ColumnTypes.TimestampWithTimezone, "2018-08-15 18:26:36+08").asInstanceOf[time.DateTime]
      dateTime.getYear === 2018
      dateTime.getMonthOfYear === 8
      dateTime.getDayOfMonth === 15
      dateTime.getHourOfDay === 10
      dateTime.getMinuteOfHour === 26
      dateTime.getSecondOfMinute === 36
    }

  }

}