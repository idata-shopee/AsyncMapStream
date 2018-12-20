package io.github.shopee.idata.asyncmapstream

import scala.concurrent.{ Await, Future, Promise, duration }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import duration._

class AsyncMapStreamTest extends org.scalatest.FunSuite {
  test("single process") {
    import AsyncMapStream._

    val expectList = 1 to 100 map ((item) => item * item)
    val expect = expectList.mkString(",")
    val mapFunction = (v: Int) => v * v

    1 to 10000 foreach { index =>
      val p          = Promise[Int]()
      val resultList = ListBuffer[Int]()
      val consumer = singleProcess[Int](mapFunction, (record) => {
        resultList.append(record)
      }, () => {
        p.trySuccess(0)
      })
      // stream
      1 to 100 foreach { item =>
        consumer.consume(item)
      }
      // end signal
      consumer.finish()
      Await.result(p.future, Duration.Inf)
      assert(resultList.length == expectList.length)
      assert(resultList.mkString(",") == expect)
    }
  }
}
