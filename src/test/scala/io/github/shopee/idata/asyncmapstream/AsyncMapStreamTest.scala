package io.github.shopee.idata.asyncmapstream

import scala.concurrent.{ Await, Future, Promise, duration }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import duration._

class AsyncMapStreamTest extends org.scalatest.FunSuite {
  import AsyncMapStream._

  test("single process") {
    val expectList  = 1 to 1000 map ((item) => item * item + "")
    val mapFunction = (v: Int) => (v * v) + ""

    1 to 1000 foreach { index =>
      val p          = Promise[Int]()
      val resultList = ListBuffer[String]()
      val consumer = singleProcess[Int, String](mapFunction, (record) => {
        resultList.append(record)
      }, () => {
        p.trySuccess(0)
      })
      // stream
      1 to 1000 foreach { item =>
        consumer.consume(item)
      }
      // end signal
      consumer.finish()
      Await.result(p.future, Duration.Inf)
      assert(resultList.length == expectList.length)
      assert(resultList == expectList)
    }
  }

  test("perf: long string operations") {
    val longText = 1 to 10000 map ((index) => s"${index}") mkString (",")

    val mapFunction =
      (v: String) => {
        var result = v.split(",").mkString(".")
        1 to 10000 foreach { _ =>
          result = result.split(".").mkString(".")
        }
        result
      }

    var t1         = System.currentTimeMillis()
    val p          = Promise[String]()
    val resultList = ListBuffer[String]()
    val consumer = singleProcess[String, String](mapFunction, (record) => {
      resultList.append(record)
    }, () => {
      p.trySuccess("")
    }, 32)
    // handle 1000 strings
    1 to 1000 foreach { _ =>
      consumer.consume(longText + ";")
    }
    // end signal
    consumer.finish()

    Await.result(p.future, Duration.Inf)
    var t2 = System.currentTimeMillis()
    println(s"concurrent map computation: ${t2 - t1} ms")

    // normal computation
    val resultList2 = ListBuffer[String]()
    var t3          = System.currentTimeMillis()
    1 to 1000 foreach { _ =>
      resultList2.append(mapFunction(longText + ";"))
    }
    var t4 = System.currentTimeMillis()
    println(s"normal map computation: ${t4 - t3} ms")

    assert(resultList == resultList2)
  }
}
