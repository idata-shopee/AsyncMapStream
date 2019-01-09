package io.github.shopee.idata.asyncmapstream

import java.util.concurrent.atomic.{ AtomicBoolean }
import scala.collection.mutable.{ ListBuffer }

/**
  *
  * heade index
  *
  * tail index
  *
  * fixed array
  */
case class CircleQueue[T](initSize: Int = 100) {
  private var size: Int       = initSize
  private var headIndex: Long = -1
  private var tailIndex: Long = -1
  private var array           = new Array[Any](size)

  private val muteLock = new AtomicBoolean(false)

  def dequeue(): T = {
    while(!muteLock.compareAndSet(false, true)) {}
    val result = tryFront() match {
      case None      => throw new Exception("circle queue is empty!")
      case Some(top) =>
        headIndex += 1
        top
    }

    // release lock
    muteLock.set(false)
    result
  }

  def enqueue(item: T): Unit = {
    tryEnlarge()

    while(!muteLock.compareAndSet(false, true)) {}
    // compare and set
    if (headIndex == -1) { // first element
      headIndex = 0
    }

    // increment and get
    tailIndex += 1

    // place element
    array(getRealPos(tailIndex)) = item

    // release lock
    muteLock.set(false)
  }

  private def tryEnlarge() = {
    while(!muteLock.compareAndSet(false, true)) {}

    if(isFull()) {
      val nextSize  = size * 2
      val nextArray = new Array[Any](nextSize)
      for (i <- headIndex to tailIndex) {
        nextArray((i - headIndex).toInt) = array(getRealPos(i))
      }

      size = nextSize
      array = nextArray
      tailIndex = tailIndex - headIndex
      headIndex = 0
    } 

    // release lock
    muteLock.set(false)
  }

  def dequeueWhen(handler: (T) => Boolean): Boolean = {
    while(!muteLock.compareAndSet(false, true)) {}

    val result = tryFront() match {
      case None => false
      case Some(item) => {
        if(handler(item)) {
          headIndex += 1
          true
        } else {
          false
        }
      }
    }

    // release lock
    muteLock.set(false)
    result
  }

  def dequeueAll(): List[T] = {
    while(!muteLock.compareAndSet(false, true)) {}

    val result = if (isEmpty()) List()
    else {
      val realHeadIndex = getRealPos(headIndex)
      val realTailIndex = getRealPos(tailIndex)
      val ret =
        if (realHeadIndex <= realTailIndex)
          array.slice(realHeadIndex, realTailIndex + 1).toList.asInstanceOf[List[T]]
        else
          (array.slice(realHeadIndex, size).toList ++ array.slice(0, realTailIndex + 1))
            .asInstanceOf[List[T]]
      headIndex = 1
      tailIndex = 0
      ret
    }

    // release lock
    muteLock.set(false)
    result
  }

  private def length(): Long = if (headIndex == -1) 0 else tailIndex - headIndex + 1

  private def tryFront(): Option[T] =
    if (isEmpty()) None else Some(array(getRealPos(headIndex)).asInstanceOf[T]) 

  private def isEmpty(): Boolean = length() == 0

  private def isFull(): Boolean = length() == size

  private def getRealPos(index: Long): Int = (index % size).toInt
}
