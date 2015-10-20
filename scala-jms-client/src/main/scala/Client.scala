import javax.jms._
import scala.util.Random
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.{ask, BackoffSupervisor}
import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.duration._

object Client {
  case class ConnectionInfo(connectionFactory: QueueConnectionFactory, queue: String)
}

class Client(connectionInfo: Client.ConnectionInfo)(as: ActorSystem) {
  import JmsActor._

  implicit val to = new Timeout(Duration(200, "milliseconds"))

  val supervisor = BackoffSupervisor.props(
    JmsActor.props(connectionInfo),
    childName = "jmsActor",
    minBackoff = 1.seconds,
    maxBackoff = 5.seconds,
    randomFactor = 0.2) // adds 20% "noise" to vary the intervals slightly

  val clientActor = as.actorOf(supervisor, name = "jmsSupervisor")
  def apply(query: String): Future[String] = (clientActor ? Send(query)).mapTo[String]
}

object JmsActor {
  case class Send(msg: String)
  case class Receive(msg: Message)
  case class Exception(exception: JMSException)

  def props(connectionInfo: Client.ConnectionInfo): Props = Props(new JmsActor(connectionInfo))
}

class JmsActor(connectionInfo: Client.ConnectionInfo) extends Actor with ActorLogging {
  log.debug("Initializing JmsActor")
  import JmsActor._

  // TODO, is this thing thread safe?

  log.debug("Creating connection")
  val connection = connectionInfo.connectionFactory.createConnection()

  connection.setExceptionListener(new ExceptionListener {
    override def onException(exception: JMSException) = self ! Exception(exception)
  })

  log.debug("Starting connection")
  connection.start()
  log.debug("Connection started")

  log.debug("Starting session")
  val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
  log.debug("Session. started")
  // Create the destination (Topic or Queue)
  log.debug("Creating destination")
  val destination = session.createQueue(connectionInfo.queue)
  log.debug("Destination created")

  log.debug("Creating producer")
  // Create a MessageConsumer from the Session to the Topic or Queue
  val producer = session.createProducer(destination)
  log.debug("Producer created")

  log.debug("Creating temporary queue")
  val tempQueue = session.createTemporaryQueue()
  log.debug("Temporary queue created")

  log.debug("Creating consumer")
  val responseConsumer = session.createConsumer(tempQueue)
  log.debug("Consumer created")

  var senders: Map[String, ActorRef] = Map.empty

  responseConsumer.setMessageListener(new MessageListener {
    override def onMessage(msg: Message) = self ! Receive(msg)
  })

  override def receive = {
    case Exception(ex: JMSException) => throw ex
    case Send(msg: String) => sendRequest(msg)
    case Receive(msg: TextMessage) => {
      senders(msg.getJMSCorrelationID) ! msg.getText
      senders -= msg.getJMSCorrelationID
    }
    case other => println("Unhandled message: " + other)
  }

  private def sendRequest(msg: String) = {
    val txtMessage = session.createTextMessage
    txtMessage.setText(msg)
    txtMessage.setJMSReplyTo(tempQueue)
    val correlationId = Random.alphanumeric.take(20).mkString
    senders += correlationId -> sender
    txtMessage.setJMSCorrelationID(correlationId)
    producer.send(txtMessage)
  }

  override def postStop() = {
    log.debug("Shutting down actor!")
    producer.close()
    responseConsumer.close()
    tempQueue.delete()
    session.close()
    connection.close()
  }
}
