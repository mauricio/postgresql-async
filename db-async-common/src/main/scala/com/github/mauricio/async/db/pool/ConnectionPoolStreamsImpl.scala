package com.github.mauricio.async.db.pool

import com.github.mauricio.async.db.{Connection, RowData}
import org.reactivestreams.{Subscription, Subscriber, Publisher}

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}

trait ConnectionPoolStreamsImpl[T <: Connection] extends AsyncObjectPool[T] {
  def executionContext: ExecutionContext

  def streamQuery(query: String, values: Seq[Any] = List(), fetchSize : Int = 0): Publisher[RowData] = {
    new Publisher[RowData] {
      override def subscribe(s: Subscriber[_ >: RowData]): Unit = {
        take.onComplete{
          case Failure(tr) => s.onError(tr)
          case Success(connection) => connection.streamQuery(query, values, fetchSize).subscribe(new Subscriber[RowData] {
            override def onError(t: Throwable): Unit = {
              giveBack(connection).onComplete{ _ =>
                s.onError(t)
              }(executionContext)
            }

            override def onSubscribe(subscription: Subscription): Unit = {
              s.onSubscribe(subscription)
            }

            override def onComplete(): Unit = {
              giveBack(connection).onComplete{
                case Failure(th) => s.onError(th)
                case _ => s.onComplete()
              }(executionContext)
            }

            override def onNext(t: RowData): Unit = {
              s.onNext(t)
            }
          })
        }(executionContext)
      }
    }
  }
}
