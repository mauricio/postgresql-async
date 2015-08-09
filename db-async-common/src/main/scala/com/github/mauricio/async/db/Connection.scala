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

package com.github.mauricio.async.db

import org.reactivestreams.Publisher

import concurrent.Future

/**
 *
 * Base interface for all objects that behave like a connection. This trait will usually be implemented by the
 * objects that connect to a database, either over the filesystem or sockets. {@link Connection} are not supposed
 * to be thread-safe and clients should assume implementations **are not** thread safe and shouldn't try to perform
 * more than one statement (either common or prepared) at the same time. They should wait for the previous statement
 * to be executed to then be able to pick the next one.
 *
 * You can, for instance, compose on top of the futures returned by this class to execute many statements
 * at the same time:
 *
 * {{{
 *   val handler: Connection = ...
 *   val result: Future[QueryResult] = handler.connect
 *     .map(parameters => handler)
 *     .flatMap(connection => connection.sendQuery("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
 *     .flatMap(query => handler.sendQuery("SELECT 0"))
 *     .flatMap(query => handler.sendQuery("COMMIT").map(value => query))
 *
 *   val queryResult: QueryResult = Await.result(result, Duration(5, SECONDS))
 * }}}
 *
 */

trait Connection {

  /**
   *
   * Disconnects this object. You should discard this object after calling this method. No more queries
   * will be accepted.
   *
   * @return
   */

  def disconnect: Future[Connection]

  /**
   *
   * Connects this object to the database. Connection objects are not necessarily created with a connection to the
   * database so you might have to call this method to be able to run queries against it.
   *
   * @return
   */

  def connect: Future[Connection]

  /**
   *
   * Checks whether we are still connected to the database.
   *
   * @return
   */

  def isConnected: Boolean

  /**
   *
   * Sends a statement to the database. The statement can be anything your database can execute. Not all statements
   * will return a collection of rows, so check the returned object if there are rows available.
   *
   * @param query
   * @return
   */

  def sendQuery(query: String): Future[QueryResult]

  /**
   *
   * Sends a prepared statement to the database. Prepared statements are special statements that are pre-compiled
   * by the database to run faster, they also allow you to avoid SQL injection attacks by not having to concatenate
   * strings from possibly unsafe sources (like users) and sending them directy to the database.
   *
   * When sending a prepared statement, you can insert ? signs in your statement and then provide values at the method
   * call 'values' parameter, as in:
   *
   * {{{
   *  connection.sendPreparedStatement( "SELECT * FROM users WHERE users.login = ?", Array( "john-doe" ) )
   * }}}
   *
   * As you are using the ? as the placeholder for the value, you don't have to perform any kind of manipulation
   * to the value, just provide it as is and the database will clean it up. You must provide as many parameters
   * as you have provided placeholders, so, if your query is as "INSERT INTO users (login,email) VALUES (?,?)" you
   * have to provide an array with at least two values, as in:
   *
   * {{{
   *   Array("john-doe", "doe@mail.com")
   * }}}
   *
   * You can still use this method if your statement doesn't take any parameters, the default is an empty collection.
   *
   * @param query
   * @param values
   * @return
   */

  def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult]

  /**
   *
   * Executes an (asynchronous) function within a transaction block.
   * If the function completes successfully, the transaction is committed, otherwise it is aborted.
   *
   * @param f operation to execute on this connection
   * @return result of f, conditional on transaction operations succeeding
   */

  def inTransaction[A](f : Connection => Future[A])(implicit executionContext : scala.concurrent.ExecutionContext) : Future[A] = {
    this.sendQuery("BEGIN").flatMap { _ =>
      val p = scala.concurrent.Promise[A]()
      f(this).onComplete { r =>
        this.sendQuery(if (r.isFailure) "ROLLBACK" else "COMMIT").onComplete {
          case scala.util.Failure(e) if r.isSuccess => p.failure(e)
          case _ => p.complete(r)
        }
      }
      p.future
    }
  }

  /**
   * Stream query creating a publisher for [[http://reactive-streams.org reactive streams]].
   * You can use one of implementations of reactive streams to work with this method, such as Akka Streams.
   * @param query query string
   * @param values parameters of query
   * @param fetchSize size of chunk to retrieve from database. It is used to implement back pressure.
   * @return A publisher
   * @see [[com.github.mauricio.async.db.Connection.sendPreparedStatement]]
   */
  def streamQuery(query: String, values: Seq[Any] = List(), fetchSize : Int = 0): Publisher[RowData]
}
