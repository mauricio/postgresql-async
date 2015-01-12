package com.github.mauricio.async.db.pool;

import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.{ QueryResult, Connection }
import scala.concurrent.{ExecutionContext, Future}

class PartitionedConnectionPool[T <: Connection](
    factory: ObjectFactory[T],
    configuration: PoolConfiguration,
    numberOfPartitions: Int)(
    implicit executionContext : ExecutionContext = ExecutorServiceUtils.CachedExecutionContext)
    extends PartitionedAsyncObjectPool[T](factory, configuration, numberOfPartitions)(executionContext)
    with Connection {

    def disconnect: Future[Connection] = if (this.isConnected) {
        this.close.map(item => this)
    } else {
        Future.successful(this)
    }

    def connect: Future[Connection] = Future.successful(this)

    def isConnected: Boolean = !this.isClosed

    def sendQuery(query: String): Future[QueryResult] =
        this.use(_.sendQuery(query))

    def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] =
        this.use(_.sendPreparedStatement(query, values))

    override def inTransaction[A](f: Connection => Future[A]): Future[A] =
        this.use(_.inTransaction[A](f))
}
