import org.apache.activemq.ActiveMQConnectionFactory
import akka.actor.ActorSystem
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._

object ClientApp extends App {
  val url = "tcp://localhost:61616"
  val queue = "test"
  val connectionFactory = new ActiveMQConnectionFactory(url)
  // val server = new Server()

  val actorSystem = ActorSystem()
  val client = new Client(Client.ConnectionInfo(connectionFactory, queue))(actorSystem)

  try {
    var stop = false
    while (!stop) {
      val query = readLine("Query: ")
      if (query == "q") stop = true
      else {
        val result = client(query)

        Try(Await.result(result, Duration.Inf)) match {
          case Success(result) => println("Response: " + result)
          case Failure(t) => println("Failure: " + t)
        }

      }
    }

  } finally {
    actorSystem.shutdown()
  }
}
