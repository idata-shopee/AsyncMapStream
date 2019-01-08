package io.github.shopee.idata.asyncmapstream

class CircleQueueTest extends org.scalatest.FunSuite {
  test("base") {
    val queue = CircleQueue[Int]() 
    1 to 100 foreach { len =>
      1 to len foreach(index => {
        queue.enqueue(index)
      })

      1 to len foreach(index => {
        assert(queue.dequeue() == index)
      })
    }
  }
}
