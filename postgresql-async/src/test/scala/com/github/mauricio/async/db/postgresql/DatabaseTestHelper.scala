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

package com.github.mauricio.async.db.postgresql

import java.util.concurrent.{TimeUnit, TimeoutException}

import com.github.mauricio.async.db.util.Log
import com.github.mauricio.async.db.{RowData, Configuration, Connection}
import org.reactivestreams.{Subscription, Subscriber}

import scala.concurrent.duration._
import scala.concurrent.{Promise, Await, Future}

object DatabaseTestHelper {
  val log = Log.get[DatabaseTestHelper]
}

trait DatabaseTestHelper {


  def databaseName = Some("netty_driver_test")

  def timeTestDatabase = Some("netty_driver_time_test")

  def databasePort = 5432

  def defaultConfiguration = new Configuration(
    port = databasePort,
    username = "postgres",
    database = databaseName)

  def timeTestConfiguration = new Configuration(
    port = databasePort,
    username = "postgres",
    database = timeTestDatabase)

  def withHandler[T](fn: (PostgreSQLConnection) => T): T = {
    withHandler(this.defaultConfiguration, fn)
  }

  def withTimeHandler[T](fn: (PostgreSQLConnection) => T): T = {
    withHandler(this.timeTestConfiguration, fn)
  }

  def withHandler[T](configuration: Configuration, fn: (PostgreSQLConnection) => T): T = {

    val handler = new PostgreSQLConnection(configuration)

    try {
      Await.result(handler.connect, Duration(5, SECONDS))
      fn(handler)
    } finally {
      handleTimeout(handler, handler.disconnect)
    }

  }

  def executeDdl(handler: Connection, data: String, count: Int = 0) = {
    val rows = handleTimeout(handler, {
      Await.result(handler.sendQuery(data), Duration(5, SECONDS)).rowsAffected
    })

    if (rows != count) {
      throw new IllegalStateException("We expected %s rows but there were %s".format(count, rows))
    }

    rows
  }

  private def handleTimeout[R]( handler : Connection, fn : => R ) = {
    try {
      fn
    } catch {
      case e : TimeoutException => {
        throw new IllegalStateException("Timeout executing call from handler -> %s".format( handler))
      }
    }
  }

  def executeQuery(handler: Connection, data: String) = {
    handleTimeout( handler, {
      Await.result(handler.sendQuery(data), Duration(5, SECONDS))
    } )
  }

  def executePreparedStatement(
                                handler: Connection,
                                statement: String,
                                values: Array[Any] = Array.empty[Any]) = {
    handleTimeout( handler, {
      Await.result(handler.sendPreparedStatement(statement, values), Duration(5, SECONDS))
    } )
  }

  def await[T](future: Future[T]): T = {
    Await.result(future, Duration(5, TimeUnit.SECONDS))
  }

  class TestSubscriber extends Subscriber[RowData] {
    val promise = Promise[IndexedSeq[RowData]]()
    override def onError(t: Throwable): Unit = {
      promise.failure(t)
    }
    
    var subscription: Option[Subscription] = None
    var requested : Long = 0
    override def onSubscribe(subscription: Subscription): Unit = {
      this.subscription = Some(subscription)
      requested = 10
      subscription.request(10)
    }

    override def onComplete(): Unit = {
      promise.success(rows)
    }

    var rows = IndexedSeq[RowData]()
    override def onNext(t: RowData): Unit = {
      rows = rows :+ t
      requested -= 1
      if (requested <= 2) {
        subscription.get.request(8)
        requested += 8
      }
    }
  }
}
