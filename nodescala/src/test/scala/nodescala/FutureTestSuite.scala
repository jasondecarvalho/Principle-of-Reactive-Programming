package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FutureTestSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Future.always converts List of Futures to Future of List, preserving order") {
    val futures: List[Future[Int]] = List(Future.always(1), Future.always(2), Future.always(3))

    val futureList: Future[List[Int]] = Future.all(futures)
    val actual = Await.result(futureList, 1 second)

    assert(actual == List(1,2,3))
  }

  test("Future.always fails if one in list fails") {
    val futures: List[Future[Int]] = List(Future.always(1), Future.never, Future.always(3))
    val futureList: Future[List[Int]] = Future.all(futures)

    //TODO refactor / use a test library that supports expected exceptions
    try {
      Await.result(futureList, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Future.delay does not return if not wait long enough") {
    val always = Future.delay(10 milliseconds)

    try {
      Await.result(always, 5 milliseconds)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Future.delay does return if wait long enough") {
    val always = Future.delay(10 milliseconds)

    Await.result(always, 1 second)
  }

  test("Future.any returns first Future to complete") {
    val futures = List(Future.never, Future.always(1), Future.delay(3 seconds))

    val actual = Await.result(Future.any(futures), 1 second)

    assert(actual == 1)
  }

  test("Future.any propagates exception") {
    val futures = List(Future.delay(1 second), Future.delay(1 second), Future { throw new Exception })

    try {
      Await.result(Future.any(futures), 2 seconds)
      assert(false)
    } catch {
      case t: TimeoutException => assert(false)
      case t: Exception => //expected
    }
  }

  test("Future.now does not block if Future unfinished") {
    try {
      Future.delay(10 seconds).now
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok
    }
  }

  test("Future.now returns if Future finished") {
    assert(Future.always(10).now == 10)
  }

  test("Future.continueWith returns if Future finished") {
    val future: Future[Int] = Future.always(10) continueWith {
      x: Future[Int] => x.now * 2
    }
    assert(Await.result(future, 1 second) == 20)
  }

  test("Future.continueWith propagates exception") {
    val future: Future[Int] = Future { throw new Exception } continueWith {
      x: Future[Int] => x.now * 2
    }

    try {
      Await.result(future, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
      case t: Exception => //expected
    }
  }

  test("Future.continue returns if Future finished") {
    val future: Future[Int] = Future.always(10) continue {
      x: Try[Int] => x.get * 2
    }
    assert(Await.result(future, 1 second) == 20)
  }

  test("Future.continue propagates exception") {
    val future: Future[Int] = Future { throw new Exception } continue {
      x: Try[Int] => x.get * 2
    }

    try {
      Await.result(future, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
      case t: Exception => //expected
    }
  }

  test("Cancellation token cancels future") {
    val cancelled = Promise[Boolean]()

    val working = Future.run() { ct =>
      Future {
        while (ct.nonCancelled) {

        }
        cancelled.success(true)
      }
    }

    val future = Future.delay(1 second) onSuccess {
      case _ => working.unsubscribe()
    }

    assert(Await.result(cancelled.future, 2 second) == true)
  }

}




