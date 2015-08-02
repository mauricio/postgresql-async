package com.github.mauricio.async.db.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}

import com.github.mauricio.async.db.RowData
import org.reactivestreams.{Subscriber, Subscription}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext


class RowDataSubscription(val subscriber: Subscriber[RowData])(implicit executionContext: ExecutionContext) extends Subscription with Runnable {
  private val on = new AtomicBoolean(false)
  private var stopped = false
  private var completed = false
  private[util] val rows = new ConcurrentLinkedQueue[RowData]()
  private val demand = new AtomicLong(0)
  subscriber.onSubscribe(this)

  override def cancel() {
    stopped = true
    tryScheduleToExecute()
  }

  override final def request(n: Long) {
    demand.addAndGet(n)
    tryScheduleToExecute()
  }

  // Should be called by one thread only
  def nextRow(rowData: RowData) {
    if (on.compareAndSet(false, true)) {
      if (!stopped) {
        if (demand.get() > 0) {
          if (rows.isEmpty) {
            subscriber.onNext(rowData)
            demand.decrementAndGet()
            on.set(false)
          } else {
            rows.offer(rowData)
            scheduleToExecute()
          }
        } else {
          rows.offer(rowData)
          on.set(false)
        }
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

  private def terminateDueTo(exception: Throwable) {
    stopped = true
    subscriber.onError(exception)
  }

  private def scheduleToExecute(): Unit = {
    try {
      executionContext.execute(this)
    } catch {
      case t: Exception =>
        if (!stopped) {
          stopped = true
          try {
            terminateDueTo(new IllegalStateException("Publisher terminated due to unavailable Executor.", t))
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

  override final def run() {
    if (demand.get() > 0) {
      sendRows()
    }
    if (completed && !stopped && rows.isEmpty) {
      subscriber.onComplete()
      stopped = true
    }
    on.set(false)
    if (stopped) {
      rows.clear()
    } else if ((demand.get() > 0 && !rows.isEmpty) || (completed && rows.isEmpty)) {
      tryScheduleToExecute()
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
