package io.github.shopee.idata.asyncmapstream

/**
 *
 * heade index
 *
 * tail index
 *
 * fixed array
 */
case class CircleQueue[T](initSize: Int = 100) {
  private var size: Int = initSize
  private var headIndex: Long = -1 
  private var tailIndex: Long = -1
  private var array = new Array[Any](size)

  val ENQUEUE = 0
  val DEQUEUE = 1

  private def mutateQueue(op: Int, data: Any): Any = synchronized {
    op match {
      case ENQUEUE => _enqueue(data.asInstanceOf[T])
      case DEQUEUE => _dequeue()
    }
  }

  def enqueue(item: T) = mutateQueue(ENQUEUE, item)

  def dequeue(): T = mutateQueue(DEQUEUE, null).asInstanceOf[T]

  def front(): T = {
    if(isEmpty()) {
      throw new Exception("circle queue is empty!")
    }

    array(getRealPos(headIndex)).asInstanceOf[T]
  }

  private def _enqueue(item: T): Unit = {
    if(isFull()) {
      // TODO
      enlarge()
      _enqueue(item)
    } else {
      if(headIndex == -1) { // first element
        headIndex = 0
      }
      tailIndex += 1

      // place element
      array(getRealPos(tailIndex)) = item
    }
  }

  private def enlarge() = {
    val nextSize = size * 2
    val nextArray = new Array[Any](nextSize)
    for(i <- headIndex to tailIndex) {
      nextArray((i - headIndex).toInt) = array(getRealPos(i))
    }

    size = nextSize
    array = nextArray
    tailIndex = tailIndex - headIndex
    headIndex = 0
  }

  private def _dequeue(): T = {
    val top = front()
    headIndex += 1
    top
  }

  def length(): Long = if(headIndex == -1) 0 else tailIndex - headIndex + 1

  def isEmpty(): Boolean = length() == 0

  def isFull(): Boolean = length() == size

  private def getRealPos(index: Long): Int = {
    (index % size).toInt
  } 
}
