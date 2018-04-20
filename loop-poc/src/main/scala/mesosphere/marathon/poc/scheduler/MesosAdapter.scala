package mesosphere.marathon
package poc.scheduler

import akka.Done
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID
import mesosphere.mesos.client.{ MesosCalls, MesosClient }
import org.apache.mesos.v1.mesos.{ AgentID, Offer, TaskID }
import org.apache.mesos.v1.scheduler.scheduler.{ Call, Event }
import scala.concurrent.Future
import scala.concurrent.duration._

object MesosAdapter extends StrictLogging {
  // Could also manage subscriptions
  def offerReviver(calls: MesosCalls, reviveRate: FiniteDuration = 5.seconds) = Flow[SchedulerLogic.MesosEffect].
    collect { case e: SchedulerLogic.Effect.WantOffers => e }.
    groupedWithin(Int.MaxValue, reviveRate)
    .mapConcat { offersWanted =>
      if (offersWanted.isEmpty)
        Nil
      else
        Seq(calls.newRevive(None))
    }

  def eventResponder(calls: MesosCalls) = Flow[SchedulerLogic.MesosEffect].mapConcat {
    case SchedulerLogic.Effect.KillTask(taskId, agent) =>
      Seq(
        calls.newKill(TaskID(taskId.asString), agent.map(AgentID(_)), killPolicy = None))
    case _: SchedulerLogic.Effect.WantOffers =>
      // handled by offerReviver
      Nil
  }

  val eventToSchedulerEvent = Flow[Event].mapConcat {
    case e: Event if e.`type` == Event.Type.OFFERS =>
      e.offers.get.offers.map { o =>
        SchedulerLogicInputEvent.MesosOffer(o)
      }(collection.breakOut)

    case e: Event if e.`type` == Event.Type.UPDATE =>
      val u = e.update.get.status
      MesosTask(u) match {
        case Some(mt) =>
          Seq(SchedulerLogicInputEvent.MesosTaskStatus(mt))
        case None =>
          logger.warn(s"Received Mesos Update without an agent... dropping. ${u}")
          Nil
      }

    case _ =>
      // ignore for now lol
      Nil
  }

  /**
    * Given a mesosClient, return a flow which wires up SchedulerLogic and sends the appropriate Mesos calls, and then
    * reads the updates from the mesosClient and maps to the appropriate SchedulerLogicInputEvent
    */
  def graph(client: MesosClient, reviveRate: FiniteDuration = 5.seconds): Flow[SchedulerLogic.MesosEffect, SchedulerLogicInputEvent, Future[Done]] = {
    Flow.fromGraph {
      GraphDSL.create(client.mesosSink, client.mesosSource)((done, _) => done) { implicit b =>
        { (eventPublisher, events) =>
          import GraphDSL.Implicits._

          val mesosEventToSchedulerEvent = b.add(eventToSchedulerEvent)
          val schedulerEffects = b.add(Flow[SchedulerLogic.MesosEffect])
          val mesosCallBroadcast = b.add(Broadcast[SchedulerLogic.MesosEffect](2))
          val mesosCallFanin = b.add(Merge[Call](2))

          schedulerEffects ~> mesosCallBroadcast
          mesosCallBroadcast ~> offerReviver(client.calls, reviveRate) ~> mesosCallFanin
          mesosCallBroadcast ~> eventResponder(client.calls) ~> mesosCallFanin

          mesosCallFanin ~> eventPublisher

          events ~> mesosEventToSchedulerEvent

          FlowShape(schedulerEffects.in, mesosEventToSchedulerEvent.out)
        }
      }
    }
  }
}
