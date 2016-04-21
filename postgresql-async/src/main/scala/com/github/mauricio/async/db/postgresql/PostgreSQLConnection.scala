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

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.github.mauricio.async.db.column.{ColumnDecoderRegistry, ColumnEncoderRegistry}
import com.github.mauricio.async.db.exceptions.{ConnectionStillRunningQueryException, InsufficientParametersException}
import com.github.mauricio.async.db.general.MutableResultSet
import com.github.mauricio.async.db.pool.TimeoutScheduler
import com.github.mauricio.async.db.postgresql.codec.{PostgreSQLConnectionDelegate, PostgreSQLConnectionHandler}
import com.github.mauricio.async.db.postgresql.column.{PostgreSQLColumnDecoderRegistry, PostgreSQLColumnEncoderRegistry}
import com.github.mauricio.async.db.postgresql.exceptions._
import com.github.mauricio.async.db.postgresql.messages.backend._
import com.github.mauricio.async.db.postgresql.messages.frontend._
import com.github.mauricio.async.db.util._
import com.github.mauricio.async.db.{Configuration, Connection, QueryResult}
import io.netty.channel.EventLoopGroup

import scala.concurrent._

object PostgreSQLConnection {
  final val Counter = new AtomicLong()
  final val ServerVersionKey = "server_version"
  final val log = Log.get[PostgreSQLConnection]
}

class PostgreSQLConnection
(
  configuration: Configuration = Configuration.Default,
  encoderRegistry: ColumnEncoderRegistry = PostgreSQLColumnEncoderRegistry.Instance,
  decoderRegistry: ColumnDecoderRegistry = PostgreSQLColumnDecoderRegistry.Instance,
  group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
  implicit val executionContext : ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
  )
  extends PostgreSQLConnectionDelegate
  with Connection
  with  TimeoutScheduler {

  import PostgreSQLConnection._

  private final val connectionHandler = new PostgreSQLConnectionHandler(
    configuration,
    encoderRegistry,
    decoderRegistry,
    this,
    group,
    executionContext
  )

  private final val currentCount = Counter.incrementAndGet()
  private final val preparedStatementsCounter = new AtomicInteger()

  private val parameterStatus = new scala.collection.mutable.HashMap[String, String]()
  private val parsedStatements = new scala.collection.mutable.HashMap[String, PreparedStatementHolder]()
  private var authenticated = false

  private var recentError : Option[Throwable] = None
  private val resultProcessorReference = new AtomicReference[Option[ResultProcessor]](None)
  private var currentPreparedStatement: Option[PreparedStatementHolder] = None
  private var version = Version(0,0,0)
  private val notifyListeners = new CopyOnWriteArrayList[NotificationResponse => Unit]()
  private var portalSuspended = false

  override def eventLoopGroup : EventLoopGroup = group
  
  private def resultProcessor: Option[ResultProcessor] = resultProcessorReference.get()

  def isReadyForQuery: Boolean = this.resultProcessor.isEmpty

  def connect: Future[Connection] = {
    val processor = new ConnectResultProcessor()
    setResultProcessor(processor)
    this.connectionHandler.connect.onFailure {
      case e => processor.failure(e)
    }

    processor.connectionPromise.future
  }

  override def disconnect: Future[Connection] = this.connectionHandler.disconnect.map( c => this )
  override def onTimeout = disconnect

  override def isConnected: Boolean = this.connectionHandler.isConnected

  def parameterStatuses: scala.collection.immutable.Map[String, String] = this.parameterStatus.toMap

  override def sendQuery(query: String): Future[QueryResult] = {
    validateQuery(query)

    val promise = Promise[QueryResult]()
    this.setResultProcessor(new PromiseResultProcessor(promise))

    write(new QueryMessage(query))
    addTimeout(promise,configuration.queryTimeout)
    promise.future
  }

  override def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] = {
    validateQuery(query)

    val promise = Promise[QueryResult]()
    val processor: PromiseResultProcessor = new PromiseResultProcessor(promise)
    this.setResultProcessor(processor)

    val holder = this.parsedStatements.getOrElseUpdate(query,
      new PreparedStatementHolder( query, preparedStatementsCounter.incrementAndGet ))

    if (holder.paramsCount != values.length) {
      this.clearResultProcessor
      throw new InsufficientParametersException(holder.paramsCount, values)
    }

    this.currentPreparedStatement = Some(holder)
    processor.columnTypes = holder.columnDatas
    write(
      if (holder.prepared)
        new PreparedStatementExecuteMessage(holder.statementId, holder.realQuery, values, this.encoderRegistry)
      else {
        holder.prepared = true
        new PreparedStatementOpeningMessage(holder.statementId, holder.realQuery, values, this.encoderRegistry)
      })
    addTimeout(promise,configuration.queryTimeout)
    promise.future
  }

  override def onError(exception : Throwable) {
    recentError = Some(exception)
  }

  def hasRecentError: Boolean = this.recentError.isDefined

  private def setErrorOnFutures(e: Throwable) {
    this.portalSuspended = false
    this.currentPreparedStatement.map(p => this.parsedStatements.remove(p.query))
    this.currentPreparedStatement = None
    this.failQuery(e)
  }

  override def onReadyForQuery() {
    if (recentError.isDefined) {
      setErrorOnFutures(recentError.get)
      this.recentError = None
    } else if (portalSuspended) {
      portalSuspended = false
      resultProcessor.get.portalSuspended()
    } else {
      this.clearResultProcessor.complete()
    }
  }

  override def onError(m: ErrorMessage) {
    log.error("Error with message -> {}", m)

    val error = new GenericDatabaseException(m)
    error.fillInStackTrace()

    recentError = Some(error)
  }

  override def onCommandComplete(m: CommandCompleteMessage) {
    resultProcessor.get.completeCommand(m.rowsAffected, m.statusMessage)
    this.currentPreparedStatement = None
  }

  override def onParameterStatus(m: ParameterStatusMessage) {
    this.parameterStatus.put(m.key, m.value)
    if ( ServerVersionKey == m.key ) {
      this.version = Version(m.value)
    }
  }

  override def onDataRow(m: DataRowMessage) {
    val items = new Array[Any](m.values.length)
    var x = 0

    val processor: ResultProcessor = this.resultProcessor.get
    val columnsData = processor.columnTypes
    while ( x < m.values.length ) {
      items(x) = if ( m.values(x) == null ) {
        null
      } else {
        val columnType = columnsData(x)
        this.decoderRegistry.decode(columnType, m.values(x), configuration.charset)
      }
      x += 1
    }

    processor.processRow(items)
  }

  override def onRowDescription(m: RowDescriptionMessage) {
    this.resultProcessor.get.columnTypes = m.columnDatas
    this.setColumnDatas(m.columnDatas)
  }

  private def setColumnDatas( columnDatas : Array[PostgreSQLColumnData] ) {
    this.currentPreparedStatement.foreach { holder =>
      holder.columnDatas = columnDatas
    }
  }

  override def onAuthenticationResponse(message: AuthenticationMessage) {
    message match {
      case m: AuthenticationOkMessage =>
        log.debug("Successfully logged in to database")
        this.authenticated = true
      case m: AuthenticationChallengeCleartextMessage =>
        write(this.credential(m))
      case m: AuthenticationChallengeMD5 =>
        write(this.credential(m))
    }
  }

  override def onNotificationResponse( message : NotificationResponse ) {
    val iterator = this.notifyListeners.iterator()
    while ( iterator.hasNext ) {
      iterator.next().apply(message)
    }
  }

  def registerNotifyListener( listener : NotificationResponse => Unit ) {
    this.notifyListeners.add(listener)
  }

  def unregisterNotifyListener( listener : NotificationResponse => Unit ) {
    this.notifyListeners.remove(listener)
  }

  def clearNotifyListeners() {
    this.notifyListeners.clear()
  }

  private def credential(authenticationMessage: AuthenticationChallengeMessage): CredentialMessage = {
    if (configuration.username != null && configuration.password.isDefined) {
      new CredentialMessage(
        configuration.username,
        configuration.password.get,
        authenticationMessage.challengeType,
        authenticationMessage.salt
      )
    } else {
      throw new MissingCredentialInformationException(
        this.configuration.username,
        this.configuration.password,
        authenticationMessage.challengeType)
    }
  }

  private[this] def notReadyForQueryError(errorMessage : String, race : Boolean) = {
    log.error(errorMessage)
    throw new ConnectionStillRunningQueryException(
      this.currentCount,
      race
    )
  }
  
  def validateIfItIsReadyForQuery(errorMessage: String) =
    if (this.resultProcessor.isDefined)
      notReadyForQueryError(errorMessage, race = false)
  
  private def validateQuery(query: String) {
    this.validateIfItIsReadyForQuery("Can't run query because there is one query pending already")

    if (query == null || query.isEmpty) {
      throw new QueryMustNotBeNullOrEmptyException(query)
    }
  }

  private def setResultProcessor(resultProcessor: ResultProcessor) {
    if (!this.resultProcessorReference.compareAndSet(None, Some(resultProcessor)))
      notReadyForQueryError("Can't run query due to a race with another started query", race = true)
  }

  private def clearResultProcessor : ResultProcessor = {
    this.resultProcessorReference.getAndSet(None).get
  }

  private def failQuery(t: Throwable) {
    this.clearResultProcessor.failure(t)
  }

  private def write( message : ClientMessage ) {
    this.connectionHandler.write(message)
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName}{counter=${this.currentCount}}"
  }

  private sealed trait ResultProcessor {
    var columnTypes: IndexedSeq[PostgreSQLColumnData]

    def processRow(items: Array[Any])

    def complete()

    def portalSuspended()

    def failure(t: Throwable)

    def completeCommand(rowsAffected: Int, statusMessage: String)
  }

  private class ConnectResultProcessor extends ResultProcessor{
    val connectionPromise = Promise[Connection]()
    override var columnTypes: IndexedSeq[PostgreSQLColumnData] = _

    override def processRow(items: Array[Any]): Unit = {}

    override def completeCommand(rowsAffected: Int, statusMessage: String): Unit = {}

    override def portalSuspended(): Unit = {}

    override def failure(t: Throwable): Unit = {
      connectionPromise.failure(t)
    }

    override def complete(): Unit = {
      connectionPromise.success(PostgreSQLConnection.this)
    }
  }

  private class PromiseResultProcessor(val queryPromise : Promise[QueryResult]) extends ResultProcessor {
    var currentQuery: Option[MutableResultSet[PostgreSQLColumnData]] = None
    var queryResult: Option[QueryResult] = None

    def columnTypes : IndexedSeq[PostgreSQLColumnData] = currentQuery.get.columnTypes
    def columnTypes_= (columnData: IndexedSeq[PostgreSQLColumnData]) : Unit = {
      currentQuery = Some(new MutableResultSet(columnData))
    }
    override def complete(): Unit = {
      queryResult.foreach(queryPromise.success)
    }

    override def processRow(items: Array[Any]): Unit = {
      currentQuery.get.addRow(items)
    }

    override def failure(t: Throwable): Unit = {
      queryPromise.failure(t)
    }

    override def completeCommand(rowsAffected: Int, statusMessage: String): Unit = {
      queryResult = Some(new QueryResult(rowsAffected, statusMessage, currentQuery))
    }

    override def portalSuspended(): Unit = {}
  }
}
