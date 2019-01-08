package io.github.shopee.idata.asyncmapstream

import java.util.concurrent.Executors
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
  */
// TODO error and close
object AsyncMapStream {
  val SIGNAL_END      = 1
  val SIGNAL_HOLD     = 2
  val SIGNAL_RESOLVED = 3

  type MapFunction[T, U] = (T) => U
  type ItemHandler[U]    = (U) => Any
  type EndHandler        = () => Any
  type resolveCallback   = (Exception) => _

  case class ConsumerSignal(var signal: Int = SIGNAL_HOLD,
                            var extra: Any = null,
                            resolveCallback: resolveCallback) {
    def resolve(err: Exception, data: Any) = {
      extra = data
      signal = SIGNAL_RESOLVED

      resolveCallback(err)
    }
  }

  type Mapper = (ConsumerSignal) => _

  // TODO failure tolerant
  case class Consumer[T, U](mapper: Mapper,
                            itemHandler: ItemHandler[U],
                            endHandler: EndHandler,
                            // TODO error handler
                            buckets: Int = 8)(
      implicit ec: ExecutionContext
  ) {
    private val bucketQueues = 1 to buckets map (
        (item) => new SynchronizedQueue[ConsumerSignal]()
    )

    private var inputPointer: Int = 0

    /**
      * stream is ordered, output one by one.
      * Consume record one by one too.
      */
    def consume(record: T): Unit = {
      // push a holder to queue
      val signal = ConsumerSignal(SIGNAL_HOLD, record, resolveCallback)
      bucketQueues(inputPointer).enqueue(signal)

      mapper(signal)

      // to next bucket
      inputPointer = (inputPointer + 1) % buckets
    }

    def resolveCallback(err: Exception) =
      if (err != null) {
        // TODO
      } else {
        collect()
      }

    private var outputPointer = 0 // rotate outputPointer from 0 to buckets - 1

    /**
     * collect result from queues
     */
    private def collect() = synchronized {
      // output record
      var pointedQueue = bucketQueues(outputPointer)
      var cont         = true

      while (cont && pointedQueue.length > 0) {
        val signal = pointedQueue.front

        signal.signal match {
          case SIGNAL_HOLD => {
            // break the loop
            cont = false
          }

          case SIGNAL_RESOLVED => {
            pointedQueue.dequeue()
            itemHandler(signal.extra.asInstanceOf[U])
          }

          case SIGNAL_END => {
            pointedQueue.dequeue()
            endHandler()
          }
        }

        if (cont) {
          // rotate outputPointer
          outputPointer = (outputPointer + 1) % buckets
          pointedQueue = bucketQueues(outputPointer)
        }
      }
    }

    // finished stream signal
    def finish() = {
      bucketQueues(inputPointer).enqueue(ConsumerSignal(SIGNAL_END, null, resolveCallback))
      collect()

      // to next bucket
      inputPointer = (inputPointer + 1) % buckets
    }
  }

  case class LocalSerialMapper[T, U](mapFunction: MapFunction[T, U])(
      implicit ec: ExecutionContext
  ) {
    val queue = new CircleQueue[ConsumerSignal]()

    var isProcessing = false

    private def mapQueue() = synchronized {
      if (!isProcessing) {
        // lock
        isProcessing = true
        Future {
          // println(s"queue length: ${queue.length}, thread id is: ${Thread.currentThread().getId()}")
          while (queue.length > 0) {
            val signal = queue.dequeue()
            try {
              signal.resolve(null, mapFunction(signal.extra.asInstanceOf[T]))
            } catch {
              case e: Exception => signal.resolve(e, null)
            }
          }
          // release
          isProcessing = false
        }
      }
    }

    val mapper: Mapper = (signal: ConsumerSignal) => {
      queue.enqueue(signal)
      mapQueue()
    }
  }

  /**
    * special case for local process computation
    */
  case class LocalProcesser[T, U](buckets: Int = 8, executor: ExecutionContext = null) {
    implicit val ec = if (executor == null) {
      // default thread pool
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(buckets))
    } else {
      executor
    }

    def process(mapFunction: MapFunction[T, U],
                itemHandler: ItemHandler[U],
                endHandler: EndHandler): Consumer[T, U] = {
      val localSerialMappers = 1 to buckets map { _ =>
        LocalSerialMapper(mapFunction)
      }
      var pointer = 0

      val mapper: Mapper = (signal: ConsumerSignal) => {
        localSerialMappers(pointer).mapper(signal)

        pointer += 1
        pointer = pointer % buckets
      }

      val consumer = Consumer[T, U](mapper, itemHandler, endHandler, buckets)

      consumer
    }
  }
}
