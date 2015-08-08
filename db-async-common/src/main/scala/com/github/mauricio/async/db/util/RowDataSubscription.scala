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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}

import com.github.mauricio.async.db.RowData
import org.reactivestreams.{Subscriber, Subscription}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

trait RowDataSubscriptionDelegate {
  def start(subscription: RowDataSubscription)
  def cancel(subscription: RowDataSubscription)
  def pause(subscription: RowDataSubscription)
  def continue(subscription: RowDataSubscription)
}

final class RowDataSubscription(val subscriber: Subscriber[_ >: RowData], val delegate: RowDataSubscriptionDelegate, val bufferSize : Int)
                               (implicit executionContext: ExecutionContext) extends Subscription with Runnable
{
  private val on = new AtomicBoolean(false)
  private val needToCallCancel = new AtomicBoolean(false)
  private var started = false
  private var _paused = false
  @volatile private var stopped = false
  private var completed = false
  private[util] val rows = new ConcurrentLinkedQueue[RowData]()
  private val demand = new AtomicLong(0)
  subscriber.onSubscribe(this)

  def paused : Boolean = _paused

  override def cancel() {
    needToCallCancel.set(true)
    stopped = true
    tryScheduleToExecute()
  }

  override def request(n: Long) {
    if (n <= 0) {
      terminate(new IllegalArgumentException(s"Requested number $n <= 0"))
    } else {
      demand.addAndGet(n)
      tryScheduleToExecute()
    }
  }

  // Should be called by one thread only
  def nextRow(rowData: RowData) {
    if (on.compareAndSet(false, true)) {
      if (!stopped) {
        if (demand.get() > 0) {
          if (rows.isEmpty) {
            if (!started) {
              delegate.start(this)
              started = true
            }
            subscriber.onNext(rowData)
            demand.decrementAndGet()
            on.set(false)
          } else {
            rows.offer(rowData)
            scheduleToExecute()
          }
        } else {
          rows.offer(rowData)
          if (!_paused && rows.size() >= bufferSize) {
            _paused = true
            delegate.pause(this)
          }
          on.set(false)
        }
      }
      if (stopped && needToCallCancel.compareAndSet(true, false)) {
        delegate.cancel(this)
      }
    } else if (!stopped) {
      rows.offer(rowData)
      tryScheduleToExecute()
    }
  }

  def complete(): Unit = {
    if (on.compareAndSet(false, true)) {
      completed = true
      if (!stopped) {
        if (rows.isEmpty) {
          subscriber.onComplete()
          stopped = true
          on.set(false)
        } else {
          scheduleToExecute()
        }
      }
    } else {
      completed = true
      tryScheduleToExecute()
    }
  }

  private var exception : Option[Throwable] = None
  def terminate(exception: Throwable) {
    stopped = true
    this.exception = Some(exception)
    needToCallCancel.set(true)
    tryScheduleToExecute()
  }

  private def scheduleToExecute(): Unit = {
    try {
      executionContext.execute(this)
    } catch {
      case t: Exception =>
        if (!stopped) {
          stopped = true
          try {
            terminate(new IllegalStateException("Publisher terminated due to unavailable Executor.", t))
          } finally {
            rows.clear()
            on.set(false)
          }
        }
    }
  }

  private def tryScheduleToExecute() {
    if (on.compareAndSet(false, true)) {
      scheduleToExecute()
    }
  }

  override def run() {
    if (demand.get() > 0) {
      if (!started) {
        delegate.start(this)
        started = true
      }
      if (_paused) {
        delegate.continue(this)
        _paused = false
      }
      sendRows()
    }
    if (completed && !stopped && rows.isEmpty) {
      subscriber.onComplete()
      stopped = true
    }
    on.set(false)
    if (stopped) {
      if (needToCallCancel.compareAndSet(true, false)) {
        delegate.cancel(this)
        exception.foreach(subscriber.onError)
      }
      rows.clear()
    } else {
      if ((demand.get() > 0 && !rows.isEmpty) || (completed && rows.isEmpty)) {
        tryScheduleToExecute()
      }
    }
  }

  @tailrec private def sendRows(): Unit = {
    if (!stopped) {
      val row = rows.poll()
      if (row != null) {
        subscriber.onNext(row)
        if (demand.decrementAndGet() > 0) {
          sendRows()
        }
      }
    }
  }
}
