package io.github.shopee.idata.asyncmapstream

import scala.collection.mutable.SynchronizedQueue
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{ Future }
import scala.concurrent.ExecutionContext

/**
  * model:
  *   stream s1 - map computation - stream s2
  *
  * problem:
  *   if stream s1 is fast, but computation is slow, may cause blocking problem.
  *
  *
  * producer: fast stream
  *
  * calculator, map function: (T) -> T
  *
  * collector: collect results, and output as a stream
  */
case class AsyncMapStreamRecord(record: Any, signal: Int)

object AsyncMapStream {
  type MapFunction[T] = (T) => T

  val SIGNAL_ITEM = 0
  val SIGNAL_END  = 1

  case class SendingQueue[T](bucketIndex: Int, sendItem: (AsyncMapStreamRecord, Int) => Future[_])(
      implicit ec: ExecutionContext
  ) {
    private val queue = new SynchronizedQueue[AsyncMapStreamRecord]()

    def enqueue(record: AsyncMapStreamRecord) = {
      queue.enqueue(record)
      sendRecord()
    }

    private var prevSending: Future[_] = null

    private def sendRecord(): Unit = synchronized {
      if (queue.length > 0 && prevSending == null) {
        val record = queue.dequeue()
        // send current record to a map calculator
        prevSending = sendItem(record, bucketIndex)

        prevSending map { _ =>
          prevSending = null
          sendRecord()
        }
      }
    }
  }

  /**
    * responsible for map computation of stream records
    */
  case class MapCalculator[T](
      mapFunction: MapFunction[T],
      bucketIndex: Int,
      gather: (AsyncMapStreamRecord, Int) => Future[_]
  )(implicit ec: ExecutionContext) {
    private val sendingQueue = SendingQueue(bucketIndex, gather)

    def calculate(record: AsyncMapStreamRecord) =
      record.signal match {
        case SIGNAL_ITEM =>
          val result = mapFunction(record.record.asInstanceOf[T])
          // send result to collector
          sendingQueue.enqueue(AsyncMapStreamRecord(result, SIGNAL_ITEM))
        case SIGNAL_END =>
          sendingQueue.enqueue(record)
      }
  }

  // TODO failure tolerant
  case class Consumer[T](buckets: Int, sendItem: (AsyncMapStreamRecord, Int) => Future[_])(
      implicit ec: ExecutionContext
  ) {
    private var currentIndex: Long = 0

    private val bucketQueues = 0 to buckets - 1 map (
        (bucketIndex) => SendingQueue[T](bucketIndex, sendItem)
    )

    /**
      * stream is ordered, output one by one.
      * Consume record one by one too.
      */
    def consume(record: T): Unit = synchronized {
      consumeHelp(AsyncMapStreamRecord(record, SIGNAL_ITEM))
    }

    private def consumeHelp(record: AsyncMapStreamRecord): Unit = synchronized {
      val bucketIndex = (currentIndex % buckets).toInt
      bucketQueues(bucketIndex).enqueue(record)
      currentIndex += 1
    }

    // finished stream signal
    def finish() =
      consumeHelp(AsyncMapStreamRecord(null, SIGNAL_END))
  }

  /**
    * collect consumer's result and output as a new stream with the same input order
    */
  case class Collecter[T](buckets: Int, itemHandler: (T) => Any, endHandler: () => Any) {
    private val bucketQueues = 1 to buckets map (
        (item) => new SynchronizedQueue[AsyncMapStreamRecord]()
    )
    private var pointer = 0 // rotate pointer from 0 to buckets - 1

    def collect(recordResult: AsyncMapStreamRecord, bucketIndex: Int) = {
      // push record result to specified bucket
      bucketQueues(bucketIndex).enqueue(recordResult)
      // output record
      outputRecord()
    }

    private def outputRecord(): Unit = synchronized {
      val pointedQueue = bucketQueues(pointer)

      if (pointedQueue.length > 0) {
        val record = pointedQueue.dequeue()
        record.signal match {
          case SIGNAL_ITEM =>
            try {
              itemHandler(record.record.asInstanceOf[T])
            } catch {
              case e: Exception => {
                //
              }
            }
          case SIGNAL_END =>
            endHandler()
        }
        // rotate pointer
        pointer += 1
        pointer = pointer % buckets

        outputRecord()
      }
    }
  }

  /**
    * special case for single process computation
    */
  def singleProcess[T](mapFunction: MapFunction[T],
                       itemHandler: (T) => Any,
                       endHandler: () => Any,
                       buckets: Int = 8)(implicit ec: ExecutionContext): Consumer[T] = {

    // collector
    val collecter = Collecter[T](buckets, itemHandler, endHandler)

    // create some calculators
    val gather = (recordResult: AsyncMapStreamRecord, bucketIndex: Int) => {
      Future {
        collecter.collect(recordResult, bucketIndex)
      }
    }
    val cals = 0 to buckets - 1 map (
        (bucketIndex) => MapCalculator[T](mapFunction, bucketIndex, gather)
    )

    // consumer
    val sendItem = (record: AsyncMapStreamRecord, bucketIndex: Int) => {
      Future {
        cals(bucketIndex).calculate(record)
      }
    }

    Consumer[T](buckets, sendItem)
  }
}
