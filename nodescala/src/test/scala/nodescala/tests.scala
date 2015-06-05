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
import org.scalatest.concurrent.AsyncAssertions

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite with ShouldMatchers with AsyncAssertions {

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

  
test("continue with basic") {
    val f = Future.always(2).continueWith { x => 3 }
    val actual = Await.result(f, 1 second)
    actual == 3
  }
  test("continue basic") {
    val f = Future.always(2).continue { x => x.get+1 }
    val actual = Await.result(f, 1 second)
    actual == 3
  }
  test("Future.continueWith should handle exceptions thrown by the user specified continuation function") {
    val f = Future.always(1)
    val c1 = f continueWith { _ => throw new UnsupportedOperationException("...") }
    intercept[java.lang.UnsupportedOperationException] {
      Await.result(c1, 1 second)
    }
  }
   test("Future.continue should handle exceptions thrown by the user specified continuation function") {
    val f = Future.always(1)
    val c1 = f continue { _ => throw new UnsupportedOperationException("...") }
    intercept[java.lang.UnsupportedOperationException] {
      Await.result(c1, 1 second)
    }
   }

    test("continueWith simple") 
  { 
    val fS = Future { 3 }
      .continueWith(fT => (Await.result(fT, 1 seconds) + 1).toString)

    assert(Await.result(fS, 2 seconds) equals "4")
  }  
  
test("A Future should be completed after 1s delay") { 
    val start = System.currentTimeMillis()  

    Future.delay(1 second) onComplete { case _ =>  
      val duration = System.currentTimeMillis() - start 
      assert (duration >= 1000L && duration < 1100L)
    }
  }  

  test("Two sequential delays of 1s should delay by 2s") {  
    val start = System.currentTimeMillis()  

    val combined = for {  
      f1 <- Future.delay(1 second)  
      f2 <- Future.delay(1 second)  
    } yield ()  

    combined onComplete { case _ =>  
      val duration = System.currentTimeMillis() - start  
      assert (duration >= 2000L && duration < 2100L)
    }
  }    
  test("A Future should not complete after 2s when using a delay of 5s") {
    try {
      val p = Future.delay(5 second)
      val z = Await.result(p, 2 second) // block for future to complete
      assert(false)
    } catch {
      case _: TimeoutException => // Ok!
    }
  }

  test("Future.run should run the future unless cancelled from subscriber") {
    val promise = Promise[String]()
    val working = Future.run() {
      ct => 
        Future {
          while (ct.nonCancelled) {
            // working
          }
          promise.trySuccess("done")
        }
    }
    assert(promise.future.value == None)
    working.unsubscribe
    assert(promise.future.now == "done")
 }

  
  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

test("All futures return") {
  val list: List[Future[Int]] = List(
    Future.always(1),
    Future.always(2),
    Future.delay(1 second) continueWith { _ => 3 }
  )
  val all: Future[List[Int]] = Future.all(list)
  assert(Await.result(all, 1.1 seconds).sum == 6)
}

test("Server should cancel a long-running or infinite response") {  
  val dummy = new DummyServer(8191)  
  val dummySubscription = dummy.start("/testDir") {  
    request => Iterator.continually("a")  
  }  

  // wait until server is really installed  
  Thread.sleep(500)  

  val webpage = dummy.emit("/testDir", Map("Any" -> List("thing")))  
  try {  
    // Timeout set to 1 seconds, so if the future hasn't completed after 3 seconds  
    // the timeout is not working as expected and Await.Result will throw a TimeoutException  
    val content = Await.result(webpage.loaded.future, 3 seconds)  
  } catch {  
    case e: TimeoutException => fail("Server did not cancel a long-running or infinite response")  
  }  

 // stop everything  
 dummySubscription.unsubscribe()  
}    

test("Server should be stoppable if receives infinite  response") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => Iterator.continually("a")
    }

    // wait until server is really installed
    Thread.sleep(500)

    val webpage = dummy.emit("/testDir", Map("Any" -> List("thing")))
    try {
      // let's wait some time
      Await.result(webpage.loaded.future, 1 second)
      fail("infinite response ended")
    } catch {
      case e: TimeoutException =>
    }

    // stop everything
    dummySubscription.unsubscribe()
    Thread.sleep(500)
    webpage.loaded.future.now // should not get NoSuchElementException
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




