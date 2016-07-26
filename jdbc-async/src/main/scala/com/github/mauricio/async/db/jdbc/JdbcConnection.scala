package com.github.mauricio.async.db.jdbc

import java.sql.Statement.RETURN_GENERATED_KEYS
import java.sql._
import javax.sql.DataSource

import com.github.mauricio.async.db.general.MutableResultSet
import com.github.mauricio.async.db.pool.TimeoutScheduler
import com.github.mauricio.async.db.util.NettyUtils
import com.github.mauricio.async.db.{Connection, QueryResult}
import io.netty.channel.EventLoopGroup
import org.joda.time.{DateTime, LocalDate, LocalTime}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object JdbcConnection {

  /**
    * Creates JdbcConnection using a JDBC datasource. All DB operations will be executed
    * using provided execution context.
    *
    * Statement options such as a fetch size can be set using stmtOptions java.sql.Statement consumer.
    * Connection options such as auto commit setting can be set using connOptions java.sql.Connection consumer.
    *
    * @param ds          JDBC datasource that will be used to obtain connections
    * @param ec          execution context used to execute DB operations
    * @param stmtOptions function called on every JDBC statement created to allow clients to set custom Statement options
    * @param connOptions function called on every JDBC connection to allow clients to set custom Connection options
    * @return a JdbcCnnection
    */
  def forDataSource(ds: DataSource, ec: ExecutionContext,
                    stmtOptions: Statement => Unit = _ => (),
                    connOptions: java.sql.Connection => Unit = _ => ()): JdbcConnection = {
    new JdbcConnection(() => ds.getConnection, ec, stmtOptions, connOptions)
  }

  /**
    * Creates JdbcConnection using a configuration provided. All DB operations will be
    * executed using provided execution context.
    *
    * Statement options such as a fetch size can be set using stmtOptions java.sql.Statement consumer.
    * Connection options such as auto commit setting can be set using connOptions java.sql.Connection consumer.
    *
    * @param config      a configuration used to obtain a JDBC driver
    * @param ec          execution context used to execute DB operations
    * @param stmtOptions function called on every JDBC statement created to allow clients to set custom Statement options
    * @param connOptions function called on every JDBC connection to allow clients to set custom Connection options
    * @return a JdbcConnection
    */
  def forConfig(config: JdbcConnectionConfiguration, ec: ExecutionContext,
                stmtOptions: Statement => Unit = _ => (),
                connOptions: java.sql.Connection => Unit = _ => ()): JdbcConnection = {
    val props = new java.util.Properties()
    config.driverProps.foreach { case (key, value) => props.setProperty(key, value) }
    new JdbcConnection(() => DriverManager.getConnection(config.jdbcUrl, props), ec, stmtOptions, connOptions)
  }
}

/**
  * A connection implementation that uses JDBC driver. JDBC interface is inherently blocking,
  * so DB operations will be executed using provided execution context - a calling thread won't be blocked.
  *
  * Value types returned by queries depend on a JDBC driver implementation - ResultSet.getObject method
  * is used to map database values to JVM objects. java.sql.Timestamp, java.sql.Time, java.sql.Date
  * are converted to their joda-time counterparts.
  *
  * All JDBC statements are executed with Statement.RETURN_GENERATED_KEYS option set.
  *
  * To create instances of this class use JdbcConnection companion object.
  *
  * @param jdbcConnectionProvider a provider of JDBC connections
  * @param ec                     execution context used to execute DB operations
  * @param stmtOptions            function called on every JDBC statement created to allow clients to set custom Statement options
  * @param connOptions            function called on every JDBC connection to allow clients to set custom Connection options
  */
class JdbcConnection(jdbcConnectionProvider: () => java.sql.Connection, val ec: ExecutionContext,
                     stmtOptions: Statement => Unit,
                     connOptions: java.sql.Connection => Unit,
                     group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup)
  extends Connection
  with  TimeoutScheduler {

  private var maybeJdbcConn: Option[java.sql.Connection] = None

  private val serverMessage = "" //JDBC doesn't support server messages
  private val notConnectedMessage = "Connection not open"

  /** @inheritdoc*/
  override def connect: Future[Connection] = async {
    if (isConnected) throw new IllegalStateException("Connection already open")
    else {
      maybeJdbcConn = Some(jdbcConnectionProvider())
      this
    }
  }

  /** @inheritdoc*/
  override def disconnect: Future[Connection] = async {
    if (!isConnected) throw new IllegalStateException(notConnectedMessage)
    else {
      maybeJdbcConn.foreach(_.close())
      maybeJdbcConn = None
      this
    }
  }

  /** @inheritdoc*/
  override def isConnected: Boolean = {
    maybeJdbcConn.fold(false) { jdbcConn =>
      !jdbcConn.isClosed
    }
  }

  /** @inheritdoc*/
  override def sendQuery(query: String): Future[QueryResult] = sendStatement[Statement](_.createStatement(), _.execute(query, RETURN_GENERATED_KEYS))

  /** @inheritdoc*/
  override def sendPreparedStatement(query: String, values: Seq[Any]): Future[QueryResult] = {
    sendStatement[PreparedStatement](
      jdbcConn => {
        val stmt = jdbcConn.prepareStatement(query, RETURN_GENERATED_KEYS)
        values.zipWithIndex.foreach { case (value, idx) => stmt.setObject(idx + 1, mapJavaObj(value)) }
        stmt
      },
      _.execute()
    )
  }

  /** @inheritdoc*/
  override def onTimeout: Unit = disconnect

  /** @inheritdoc*/
  override def eventLoopGroup : EventLoopGroup = group

  /**
    * Executes a JDBC statement in this connection's execution context and traverses through result set returned, if any.
    * The statement is closed after result set traversal.
    *
    * @param stmtProvider statement provider
    * @param stmtExecutor statement executor
    * @tparam S a JDBC Statement type
    * @return future of query result
    */
  protected def sendStatement[S <: Statement](stmtProvider: (java.sql.Connection) => S, stmtExecutor: S => Boolean): Future[QueryResult] = {
    maybeJdbcConn.fold(Future.failed[QueryResult](new IllegalStateException(notConnectedMessage))) { jdbcConn =>
      async {
        val stmt = stmtProvider(jdbcConn)
        try {
          if (stmtExecutor(stmt)) {
            val jdbcRs = stmt.getResultSet
            jdbcResultSetToQueryResult(jdbcRs)
          } else {
            new QueryResult(stmt.getUpdateCount, serverMessage, None)
          }
        } finally {
          stmt.close()
        }
      }
    }
  }

  private def jdbcResultSetToQueryResult(jdbcRs: java.sql.ResultSet): QueryResult = {
    val rs = mutableResultSet(jdbcRs.getMetaData)
    while (jdbcRs.next()) {
      var col = 1
      val rowArrayBuffer = new ArrayBuffer[Any]()
      while (col <= jdbcRs.getMetaData.getColumnCount) {
        rowArrayBuffer += mapJdbcObj(jdbcRs.getObject(col))
        col += 1
      }
      rs.addRow(rowArrayBuffer.toArray)
    }
    new QueryResult(rs.length, serverMessage, Some(rs))
  }

  private def mapJdbcObj(any: Any): Any = any match {
    case (d: java.sql.Timestamp) => new DateTime(d)
    case (d: java.sql.Date) => LocalDate.fromDateFields(d)
    case (t: java.sql.Time) => LocalTime.fromDateFields(t)
    case (bd: java.math.BigDecimal) => scala.math.BigDecimal(bd)
    case _ => any
  }

  private def mapJavaObj(any: Any): Any = any match {
    case (d: DateTime) => new java.sql.Timestamp(d.getMillis)
    case (d: LocalDate) => new java.sql.Date(d.toDate.getTime)
    case (t: LocalTime) => new java.sql.Time(t.toDateTimeToday.getMillis)
    case (bd: scala.math.BigDecimal) => bd.bigDecimal
    case _ => any
  }

  private def mutableResultSet(metadata: ResultSetMetaData): MutableResultSet[JdbcColumnData] = {
    val columnTypes = new ArrayBuffer[JdbcColumnData]()
    var i = 1
    while (i <= metadata.getColumnCount) {
      columnTypes += JdbcColumnData(metadata, i)
      i += 1
    }
    new MutableResultSet[JdbcColumnData](columnTypes)
  }

  private def async[T](block: => T): Future[T] = {
    Future {
      block
    }(ec)
  }
}
