/*
 * Copyright 2015 Maurício Linhares
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
  //TODO: Test for terminate
  sequential

  "Positive flow" >> {
    val delegate = new TestRowDataSubscriptionDelegate()
    val subscriber = new TestSubscriber()
    val subscription = newSubscription(subscriber, delegate, bufferSize = 10)
    "Subscription should call onSubscribe" in {
      subscriber.subscribed must beTrue
    }
    "request should not affect on rows" in {
      subscription.request(1)
      subscriber.lastRow must_== -1
    }
    "nextRow should be sent" in {
      subscription.nextRow(0)
      subscriber.lastRow must_== 0
    }
    "next rows should be preserved" in {
      subscription.nextRow(1)
      subscription.nextRow(2)
      subscription.nextRow(3)
      subscriber.lastRow must_== 0
      subscription.rows must haveSize(3)
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

  "Delegate" >> {
    val delegate = new TestRowDataSubscriptionDelegate()
    val subscriber = new TestSubscriber()
    val subscription = newSubscription(subscriber, delegate, bufferSize = 2)
    "delegate should not be started or paused" >> {
      delegate.started must beFalse
    }
    "request should not cause pause" >> {
      subscription.request(1)
      delegate.paused must beFalse
    }
    "sending 2 rows should not cause pause" >> {
      subscription.nextRow(0)
      subscription.nextRow(1)
      delegate.paused must beFalse
    }
    "sending 3nd row should not cause pause" >> {
      subscription.nextRow(1)
      delegate.paused must beTrue
    }
    "continue should be called after request" >> {
      subscription.request(1)
      delegate.paused must beFalse
    }
    "cancel should be called after cancel" >> {
      subscription.cancel()
      delegate.cancelled must beTrue
    }
  }

  "When it is canceled it should stop sending and preserving rows" >> {
    val delegate = new TestRowDataSubscriptionDelegate()
    val subscriber = new TestSubscriber()
    val subscription = newSubscription(subscriber, delegate, bufferSize = 10)
    subscription.nextRow(0)
    subscription.request(1)
    subscription.nextRow(1)
    subscription.cancel()
    subscription.nextRow(2)
    subscription.rows must haveSize(0)
    subscriber.lastRow must_== 0
    delegate.started must beTrue
    delegate.cancelled must beTrue
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
    val count = 1000
    val delegate = new TestRowDataSubscriptionDelegate() {
      override def start(subscription: RowDataSubscription): Unit = {
        super.start(subscription)
        context.execute(new Runnable {
          override def run(): Unit = {
            for (row <- 0 until count) {
              subscription.nextRow(row)
            }
            subscription.complete()
          }
        })
      }
    }
    val subscription = new RowDataSubscription(subscriber, delegate, bufferSize = 10)
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
    delegate.started must beTrue
    delegate.cancelled must beFalse
  }

  "Thread safety for cancel" ! attempts {_ =>
    val subscriber = new TestSubscriber()
    implicit val context = ExecutionContext.global
    val count = 10
    val canceledPromise = Promise[Unit]()

    val delegate = new TestRowDataSubscriptionDelegate() {
      override def start(subscription: RowDataSubscription): Unit = {
        super.start(subscription)
        context.execute(new Runnable {
          override def run(): Unit = {
            for (row <- 0 until count + 10) {
              subscription.nextRow(row)
            }
            subscription.complete()
          }
        })
      }

      override def cancel(subscription: RowDataSubscription): Unit = {
        super.cancel(subscription)
        canceledPromise.success()
      }
    }
    val subscription = new RowDataSubscription(subscriber, delegate, bufferSize = 1)
    context.execute(new Runnable {
      override def run(): Unit = {
        for (row <- 0 until count) {
          subscription.request(1)
        }
        subscription.cancel()
      }
    })
    Await.ready(canceledPromise.future, Duration(10, TimeUnit.SECONDS))

    subscriber.lastRowError must beFalse
    subscriber.completed must beFalse
    subscriber.lastRow must beLessThanOrEqualTo(count - 1)
    delegate.started must beTrue
    delegate.cancelled must beTrue
  }

  def newSubscription(subscriber : TestSubscriber, delegate : RowDataSubscriptionDelegate, bufferSize : Int) =
    new RowDataSubscription(subscriber, delegate, bufferSize)(SameThreadExecutionContext)

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

  class TestRowDataSubscriptionDelegate extends RowDataSubscriptionDelegate {
    var cancelled = false
    override def cancel(subscription: RowDataSubscription): Unit = {
      if (!started) {
        throw new IllegalStateException("Not started")
      }
      if (cancelled) {
        throw new IllegalStateException("Already canceled")
      }
      cancelled = true
    }
    var paused = false
    override def pause(subscription: RowDataSubscription): Unit = {
      if (paused) {
        throw new IllegalStateException("Already paused")
      }
      if (!started) {
        throw new IllegalStateException("Not started")
      }
      paused = true
    }

    override def continue(subscription: RowDataSubscription): Unit = {
      if (!paused) {
        throw new IllegalStateException("Not paused")
      }
      if (!started) {
        throw new IllegalStateException("Not started")
      }
      paused = false
    }

    var started = false
    override def start(subscription: RowDataSubscription): Unit = {
      started = true
    }
  }
}
