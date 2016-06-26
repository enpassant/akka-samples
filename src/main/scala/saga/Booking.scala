package saga

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory,
  ActorSystem, PoisonPill, Props, Terminated }
import com.github.nscala_time.time.Imports._
import java.util.UUID
import scala.concurrent.duration._
import scala.util.Random

case class City(name: String)
case class Reserve(date: DateTime, from: City, to: City)
case class Reserved(car: Car, room: Room, flight: Flight)
case class ReserveCar(transactId: String, date: DateTime, city: City)
case class ReserveHotel(transactId: String, date: DateTime, city: City)
case class ReserveFlight(transactId: String, date: DateTime, from: City, to: City)
trait Response
case class Car(model: String) extends Response
case class Room(number: Int) extends Response
case class Flight(seat: Int) extends Response
case class Cancel(transactId: String) extends Response

class CarRental extends Actor with ActorLogging {
  def receive = {
    case ReserveCar(transactId, date, city) =>
      val successProbality = Random.nextDouble
      if (successProbality <= 0.7) {
        val car = Car(s"Ford Galaxy [${Random.nextInt(5)}]")
        log.info(s"Reserved $car")
        sender ! car
      } else {
        sender ! Cancel(transactId)
      }
    case Cancel(transactId) =>
      log.info("Cancelled {}", transactId)
  }
}

object CarRental {
  def apply()(implicit factory: ActorRefFactory) =
    factory.actorOf(Props(new CarRental), "CarRental")
}

class Hotel extends Actor with ActorLogging {
  def receive = {
    case ReserveHotel(transactId, date, city) =>
      val successProbality = Random.nextDouble
      if (successProbality <= 0.7) {
        val room = Room(Random.nextInt(900) + 100)
        log.info(s"Reserved $room")
        sender ! room
      } else {
        sender ! Cancel(transactId)
      }
    case Cancel(transactId) =>
      log.info("Cancelled {}", transactId)
  }
}

object Hotel {
  def apply()(implicit factory: ActorRefFactory) =
    factory.actorOf(Props(new Hotel), "Hotel")
}

class Airline extends Actor with ActorLogging {
  def receive = {
    case ReserveFlight(transactId, date, from, to) =>
      val successProbality = Random.nextDouble
      if (successProbality <= 0.7) {
        val flight = Flight(Random.nextInt(400) + 100)
        log.info(s"Reserved $flight")
        sender ! flight
      } else {
        sender ! Cancel(transactId)
      }
    case Cancel(transactId) =>
      log.info("Cancelled {}", transactId)
  }
}

object Airline {
  def apply()(implicit factory: ActorRefFactory) =
    factory.actorOf(Props(new Airline), "Airline")
}

class TravelAgency(carRental: ActorRef, hotel: ActorRef, airline: ActorRef)
  extends Actor with ActorLogging
{
  def receive = {
    case Reserve(date, from, to) =>
      val transactId = UUID.randomUUID.toString
      carRental ! ReserveCar(transactId, date, to)
      hotel ! ReserveHotel(transactId, date, to)
      airline ! ReserveFlight(transactId, date, from, to)

      context become waitForResponses(sender, List())
  }

  def waitForResponses(client: ActorRef, responses: List[Response]): Receive = {
    case response: Response if responses.length < 2 =>
      context become waitForResponses(client, response :: responses)

    case response: Response =>
      val (successes, cancelled) = (response :: responses) partition {
        case cancel: Cancel => false
        case _ => true
      }
      if (cancelled.isEmpty) {
        val init: (Option[Car], Option[Room], Option[Flight]) = (None, None, None)
        val params = successes.foldLeft(init) {
          case (s, v) => v match {
            case car: Car => (Some(car), s._2, s._3)
            case room: Room => (s._1, Some(room), s._3)
            case flight: Flight => (s._1, s._2, Some(flight))
          }
        }
        client ! Reserved(params._1.get, params._2.get, params._3.get)
        end
      } else {
        val cancel = cancelled.head.asInstanceOf[Cancel]
        successes foreach {
          case car: Car =>
            carRental ! Cancel(cancel.transactId)
          case room: Room =>
            hotel ! Cancel(cancel.transactId)
          case flight: Flight =>
            airline ! Cancel(cancel.transactId)
        }
        client ! cancel
        end
      }
  }

  def end = {
    context become waitForEnd
    self ! PoisonPill
  }

  def waitForEnd: Receive = {
    case _ =>
  }
}

object TravelAgency {
  def apply(index: Int, carRental: ActorRef, hotel: ActorRef, airline: ActorRef)
    (implicit factory: ActorRefFactory) =
      factory.actorOf(Props(new TravelAgency(carRental, hotel, airline)),
        s"TravelAgency_$index")
}

class TravelAgencySequential(carRental: ActorRef, hotel: ActorRef, airline: ActorRef)
  extends Actor with ActorLogging
{
  def receive = {
    case reserve: Reserve =>
      val transactId = UUID.randomUUID.toString
      carRental ! ReserveCar(transactId, reserve.date, reserve.to)
      context become waitForCar(sender, transactId, reserve)
  }

  def waitForCar(client: ActorRef, transactId: String, reserve: Reserve): Receive = {
    case car: Car =>
      hotel ! ReserveHotel(transactId, reserve.date, reserve.to)
      context become waitForHotel(client, transactId, reserve, car)
    case cancel: Cancel =>
      client ! cancel
      end
  }

  def waitForHotel(
    client: ActorRef,
    transactId: String,
    reserve: Reserve,
    car: Car): Receive =
  {
    case room: Room =>
      airline ! ReserveFlight(transactId, reserve.date, reserve.from, reserve.to)
      context become waitForAirline(client, transactId, reserve, car, room)
    case cancel: Cancel =>
      carRental ! cancel
      client ! cancel
      end
  }

  def waitForAirline(
    client: ActorRef,
    transactId: String,
    reserve: Reserve,
    car: Car,
    room: Room): Receive =
  {
    case flight: Flight =>
      client ! Reserved(car, room, flight)
      end
    case cancel: Cancel =>
      carRental ! cancel
      hotel ! cancel
      client ! cancel
      end
  }

  def end = {
    context become waitForEnd
    self ! PoisonPill
  }

  def waitForEnd: Receive = {
    case _ =>
  }
}

object TravelAgencySequential {
  def apply(index: Int, carRental: ActorRef, hotel: ActorRef, airline: ActorRef)
    (implicit factory: ActorRefFactory) =
      factory.actorOf(Props(new TravelAgencySequential(carRental, hotel, airline)),
        s"TravelAgencySequential_$index")
}

class SampleOrders(count: Int) extends Actor with ActorLogging {
  val carRental = CarRental()
  val hotel = Hotel()
  val airline = Airline()
  context watch carRental
  context watch hotel
  context watch airline

  for (i <- 1 to count) {
    val travelAgency = TravelAgency(i, carRental, hotel, airline)
    context watch travelAgency
    travelAgency ! Reserve(DateTime.now + i.months, City("London"), City("New York"))

    val travelAgencySequential = TravelAgencySequential(i, carRental, hotel, airline)
    context watch travelAgencySequential
    travelAgencySequential ! Reserve(DateTime.now + i.months, City("London"), City("New York"))
  }

  def receive = waitForResponses(2 * count)

  def waitForResponses(count: Int): Receive = {
    case Terminated(_) if count > 1 =>
      context become waitForResponses(count - 1)
    case Terminated(_) =>
      end
    case msg =>
      log.info("Received message: {}", msg)
  }

  def end = {
    context become waitForEnd(3)
    carRental ! PoisonPill
    hotel ! PoisonPill
    airline ! PoisonPill
  }

  def waitForEnd(count: Int): Receive = {
    case Terminated(_) if count > 1 =>
      context become waitForEnd(count - 1)
    case Terminated(_) =>
      context.system.shutdown
    case _ =>
  }
}

object Booking extends App {

  val system = ActorSystem("Saga")

  val SampleOrders = system.actorOf(Props(new SampleOrders(3)), "SampleOrders")
}
