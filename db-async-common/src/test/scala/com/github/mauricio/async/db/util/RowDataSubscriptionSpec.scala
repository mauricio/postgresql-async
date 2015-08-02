package com.github.mauricio.async.db.util

import java.util.concurrent.{TimeUnit, TimeoutException}

import com.github.mauricio.async.db.RowData
import org.reactivestreams.{Subscriber, Subscription}
import org.specs2.execute.{AsResult, Result, Success}
import org.specs2.mutable.Specification
import org.specs2.specification.Fixture

import scala.concurrent._
import scala.concurrent.duration.Duration

class RowDataSubscriptionSpec extends Specification {
  sequential

  "Positive flow" >> {
    val subscriber = new TestSubscriber()
    val subscription = newSubscription(subscriber)
    "Subscription should call onSubscribe" in {
      subscriber.subscribed must beTrue
    }
    "nextRow should be preserved" in {
      subscription.nextRow(0)
      subscriber.lastRow must_== -1
      subscription.rows must haveSize(1)
    }
    "and send when it is requested by calling onNext" in {
      subscription.request(1)
      subscriber.lastRow must_== 0
    }
    "next rows should be again preserved" in {
      subscription.nextRow(1)
      subscription.nextRow(2)
      subscription.nextRow(3)
      subscriber.lastRow must_== 0
    }
    "and send when it is requested but not more than the total number requested" in {
      subscription.request(2)
      subscriber.lastRow must_== 2
    }
    "onComplete should not be called if there are rows to process" in {
      subscription.complete()
      subscriber.completed must beFalse
    }
    "and onComplete should be called when all rows are sent" in {
      subscription.request(1)
      subscriber.lastRow must_== 3
      subscriber.completed must beTrue
    }
  }

  "When it is canceled it should stop sending and preserving rows" >> {
    val subscriber = new TestSubscriber()
    val subscription = newSubscription(subscriber)
    subscription.nextRow(0)
    subscription.request(1)
    subscription.nextRow(1)
    subscription.cancel()
    subscription.nextRow(2)
    subscription.rows must haveSize(0)
    subscriber.lastRow must_== 0
  }

  val attemptsCount = 1000
  val attempts = new Fixture[Int] {
    def apply[R : AsResult](f: Int => R) = {
      (0 to attemptsCount).foldLeft(Success(): Result) { (res, i) =>
        res and AsResult(f(i))
      }
    }
  }

  "Thread safety" ! attempts {_ =>
    val subscriber = new TestSubscriber()
    implicit val context = ExecutionContext.global
    val subscription = new RowDataSubscription(subscriber)
    val count = 1000
    context.execute(new Runnable {
      override def run(): Unit = {
        for (row <- 0 until count) {
          subscription.nextRow(row)
        }
        subscription.complete()
      }
    })
    context.execute(new Runnable {
      override def run(): Unit = {
        for (row <- 0 until count) {
          subscription.request(1)
        }
      }
    })
    try {
      Await.ready(subscriber.completedPromise.future, Duration(10, TimeUnit.SECONDS))
    } catch {
      case _ : TimeoutException =>
        println(s"Subscriber not completed, actual row = ${subscriber.lastRow}")
    }

    subscriber.lastRowError must beFalse
    subscriber.completed must beTrue
    subscriber.lastRow mustEqual count - 1
  }

  "Thread safety for cancel" ! attempts {_ =>
    val subscriber = new TestSubscriber()
    implicit val context = ExecutionContext.global
    val subscription = new RowDataSubscription(subscriber)
    val count = 1000
    context.execute(new Runnable {
      override def run(): Unit = {
        for (row <- 0 until count + 10) {
          subscription.nextRow(row)
        }
        subscription.complete()
      }
    })
    val canceledPromise = Promise[Unit]()
    context.execute(new Runnable {
      override def run(): Unit = {
        for (row <- 0 until count) {
          subscription.request(1)
        }
        subscription.cancel()
        canceledPromise.success()
      }
    })
    Await.ready(canceledPromise.future, Duration(10, TimeUnit.SECONDS))

    subscriber.lastRowError must beFalse
    subscriber.completed must beFalse
    subscriber.lastRow must beLessThanOrEqualTo(count - 1)
  }

  def newSubscription(subscriber : TestSubscriber) = new RowDataSubscription(subscriber)(SameThreadExecutionContext)

  implicit def intToTestRowData(rowNumber : Int) : RowData = TestRowData(rowNumber)
  case class TestRowData(rowNumber : Int) extends RowData {
    override def apply(columnNumber: Int): Any = rowNumber
    override def apply(columnName: String): Any = rowNumber
    override def length: Int = 1
  }

  class TestSubscriber extends Subscriber[RowData] {
    var error : Option[Throwable] = None
    override def onError(t: Throwable): Unit = {
      error = Some(t)
    }

    var subscribed = false
    override def onSubscribe(s: Subscription): Unit = {
      subscribed = true
    }

    val completedPromise = Promise[Unit]()
    var completed = false
    override def onComplete(): Unit = {
      completed = true
      completedPromise.success()
    }

    var lastRowError = false
    var lastRow = -1
    override def onNext(t: RowData): Unit = {
      t match {
        case TestRowData(rowNumber) =>
          if (lastRow == rowNumber - 1) {
            lastRow = rowNumber
          } else {
            lastRowError = true
          }
      }
    }

    override def toString: String = super.toString
  }


  object SameThreadExecutionContext extends ExecutionContext {
    override def execute(runnable: Runnable): Unit = {
      runnable.run()
    }

    override def reportFailure(t: Throwable): Unit ={

    }
  }
}
