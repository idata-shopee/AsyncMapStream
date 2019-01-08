package io.github.shopee.idata.asyncmapstream

import scala.concurrent.{ Await, Future, Promise, duration }
import scala.collection.mutable.ListBuffer
import duration._

class AsyncMapStreamTest extends org.scalatest.FunSuite {
  import AsyncMapStream._

  def identityTime[T](list: List[T], log: Boolean = false) = {
    val t1         = System.currentTimeMillis()
    val resultList = ListBuffer[T]()
    list.map((item) => {
      resultList.append(item)
    })
    val t2 = System.currentTimeMillis()
    if (log) {
      println(s"map identity, only append to list buffer: ${t2 - t1}")
    }
  }

  def testLocalAsyncMapStream[T, U](list: List[T],
                                    singleProcess: LocalProcesser[T, U],
                                    mapFunction: MapFunction[T, U],
                                    log: Boolean = false) = {
    identityTime(list, log)
    val p          = Promise[Int]()
    val resultList = ListBuffer[U]()
    val t1         = System.currentTimeMillis()
    val consumer = singleProcess.process(
      mapFunction,
      (record) => {
        resultList.append(record)
      },
      () => {
        val t2 = System.currentTimeMillis()
        if (log) {
          println(s"concurrent solution: ${t2 - t1}")
        }
        p.trySuccess(0)
      }
    )

    val t6 = System.currentTimeMillis()
    // stream
    list foreach { item =>
      consumer.consume(item)
    }
    // end signal
    consumer.finish()
    val t5 = System.currentTimeMillis()
    if (log) {
      println(s"concurrent solution - source end time: ${t5 - t6}")
    }

    Await.result(p.future, Duration.Inf)

    // normal computation
    val t3           = System.currentTimeMillis()
    val expectedList = ListBuffer[U]()
    list.map(mapFunction).foreach(item => expectedList.append(item))
    val t4 = System.currentTimeMillis()

    if (log) {
      println(s"normal solution: ${t4 - t3}")
    }
    assert(resultList.length == expectedList.length)

    assert(resultList == expectedList)
  }

  test("single process") {
    val mapFunction   = (v: Int) => (v * v) + ""
    val singleProcess = LocalProcesser[Int, String]()

    1 to 1000 foreach { index =>
      testLocalAsyncMapStream[Int, String]((1 to 1000).toList, singleProcess, mapFunction)
    }
  }

  test("big integer array") {
    val mapFunction   = (v: Int) => (v.toString().split("").mkString("").toInt * v) + ""
    val singleProcess = LocalProcesser[Int, String]()

    testLocalAsyncMapStream[Int, String]((1 to 1000000).toList, singleProcess, mapFunction, true)
  }

  test("perf: long string operations") {
    val longText      = 1 to 10000 map ((index) => s"${index}") mkString (",")
    val singleProcess = LocalProcesser[String, String]()

    val mapFunction =
      (v: String) => {
        var result = v.split(",").mkString(".")
        1 to 10000 foreach { _ =>
          result = result.split(".").mkString(".")
        }
        result
      }

    testLocalAsyncMapStream[String, String]((1 to 1000).map(_ => longText + ";").toList,
                                            singleProcess,
                                            mapFunction,
                                            true)
  }
}
