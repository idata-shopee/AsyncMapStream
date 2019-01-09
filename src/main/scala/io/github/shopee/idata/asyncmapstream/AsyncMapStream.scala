package io.github.shopee.idata.asyncmapstream

import java.util.concurrent.Executors
import scala.concurrent.{ Future }
import scala.concurrent.ExecutionContext
import java.util.concurrent.atomic.{ AtomicInteger, AtomicBoolean }

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
  type EndHandler        = (Exception) => Any
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

  case class Consumer[T, U](mapper: Mapper,
                            itemHandler: ItemHandler[U],
                            endHandler: EndHandler,
                            buckets: Int = 8)(
      implicit ec: ExecutionContext
  ) {
    private val bucketQueues = 1 to buckets map (
        (item) => new CircleQueue[ConsumerSignal]()
    )

    private var inputPointer = new AtomicInteger(0)
    private var isErrored: Boolean = false

    /**
      * stream is ordered, output one by one.
      * Consume record one by one too.
      */
    def consume(record: T): Unit = {
      if (isErrored) {
        throw new Exception("continue consuming after error happened.")
      }
      // push a holder to queue
      val signal = ConsumerSignal(SIGNAL_HOLD, record, resolveCallback)
      bucketQueues(inputPointer.getAndIncrement() % buckets).enqueue(signal)
      mapper(signal)
    }

    def resolveCallback(err: Exception) =
      if (err != null) {
        isErrored = true
        endHandler(err)
      } else {
        collect()
      }

    private var outputPointer = 0 // rotate outputPointer from 0 to buckets - 1
    private val collectLock = new AtomicBoolean(false)

    /**
      * collect result from queues
      */
    private def collect() = {
      // get lock and lock on
      if(collectLock.compareAndSet(false, true)) {
        // output record
        var pointedQueue = bucketQueues(outputPointer)
        var cont         = true

        while (cont && pointedQueue.length > 0) {
          val signal = pointedQueue.front

          signal.signal match {
            case SIGNAL_HOLD =>
              cont = false

            case SIGNAL_RESOLVED =>
              pointedQueue.dequeue()
              itemHandler(signal.extra.asInstanceOf[U])

            case SIGNAL_END =>
              pointedQueue.dequeue()
              endHandler(null)
          }

          if (cont) {
            // rotate outputPointer
            outputPointer = (outputPointer + 1) % buckets
            pointedQueue = bucketQueues(outputPointer)
          }
        }

        // release lock
        collectLock.set(false)
      } 
    }

    // finished stream signal
    def finish() = {
      bucketQueues(inputPointer.getAndIncrement() % buckets).enqueue(ConsumerSignal(SIGNAL_END, null, resolveCallback))
      collect()
    }
  }

  case class LocalSerialMapper[T, U](mapFunction: MapFunction[T, U])(
      implicit ec: ExecutionContext
  ) {
    val queue = new CircleQueue[ConsumerSignal]()

    var isProcessing = new AtomicBoolean(false)

    private def mapQueue() = {
      // get lock and lock on
      if(isProcessing.compareAndSet(false, true)) {
        Future {
          // println(s"queue length: ${queue.length}, thread id is: ${Thread.currentThread().getId()}")
          var con = true
          while(con) {
            queue.tryDequeue() match {
              case None => con = false
              case Some(signal) =>
                try {
                  signal.resolve(null, mapFunction(signal.extra.asInstanceOf[T]))
                } catch {
                  case e: Exception => signal.resolve(e, null)
                }
            }
          }
          // release
          isProcessing.set(false)
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
      var pointer = new AtomicInteger(0)

      val mapper: Mapper = (signal: ConsumerSignal) => {
        localSerialMappers(pointer.getAndIncrement() % buckets).mapper(signal)
      }

      val consumer = Consumer[T, U](mapper, itemHandler, endHandler, buckets)

      consumer
    }
  }
}
