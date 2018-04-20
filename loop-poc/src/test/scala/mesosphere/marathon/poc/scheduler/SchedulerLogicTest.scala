package mesosphere.marathon
package poc.scheduler

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import java.util.UUID
import mesosphere.marathon.poc.repository.StateTransition
import mesosphere.marathon.poc.state.{ Instance, RunSpec, RunSpecRef }
import mesosphere.marathon.test.SettableClock
import org.apache.mesos.v1.mesos.{ AgentID, FrameworkID, Offer, OfferID, Resource, TaskID, TaskState, TaskStatus, Value }
import org.scalatest.Inside
import mesosphere.marathon.poc.repository.StateAuthority

class SchedulerLogicTest extends AkkaUnitTestLike with Inside {
  val sampleRunSpec = RunSpec("/lol", "blue", "2018-01-01", "sleep 100")
  val instanceId = UUID.fromString("deadbeef-c011-0123-4567-89abcdefffff")
  val sampleInstance = Instance(
    instanceId,
    sampleRunSpec.ref,
    incarnation = 1L,
    goal = Instance.Goal.Running)

  "signals that offers are wanted when a new instance is launched" in {
    val inputEvents = List(
      SchedulerLogicInputEvent.MarathonStateUpdate(
        Seq(
          StateTransition.RunSpecUpdated(
            sampleRunSpec.ref,
            Some(sampleRunSpec)),
          StateTransition.InstanceUpdated(
            sampleInstance.instanceId,
            Some(sampleInstance)))))

    val result = Source(inputEvents)
      .via(SchedulerLogic.eventProcesorFlow())
      .runWith(Sink.seq)
      .futureValue

    result.head shouldBe SchedulerLogic.Effect.WantOffers(instanceId)
  }

  "it kills unknown running tasks and expunges the task" in {
    val clock = new SettableClock

    val mesosTaskId = MesosTaskId("lol", MesosTaskId.emptyUUID, 1L)
    val agentId = "lolol"
    val status = TaskStatus(
      TaskID(mesosTaskId.asString),
      TaskState.TASK_RUNNING,
      timestamp = Some(0.0),
      agentId = Some(AgentID(agentId)))
    val Some(task) = MesosTask.apply(status)

    val inputEvents = List(
      SchedulerLogicInputEvent.MesosTaskStatus(task))

    val result = Source(inputEvents)
      .via(SchedulerLogic.eventProcesorFlow(clock))
      .runWith(Sink.seq)
      .futureValue

    inside(result(0)) {
      case SchedulerLogic.Effect.TaskUpdate(taskId, newState) =>
        taskId shouldBe mesosTaskId
    }
    inside(result(1)) {
      case SchedulerLogic.Effect.KillTask(taskId, Some(agentId)) =>
        taskId shouldBe mesosTaskId
        agentId shouldBe agentId
    }

    inside(result(2)) {
      case SchedulerLogic.Effect.TaskUpdate(taskId, None) =>
    }
  }

  "it accepts offers for tasks that are targetted to be running" in {
    val clock = new SettableClock
    val offer = Offer.apply(
      OfferID("1"),
      frameworkId = FrameworkID("1"),
      agentId = AgentID("1"),
      hostname = "1",
      resources = Seq(
        Resource(name = "cpus", `type` = Value.Type.SCALAR, scalar = Some(Value.Scalar(16.0)), role = Some("*")),
        Resource(name = "mem", `type` = Value.Type.SCALAR, scalar = Some(Value.Scalar(256.0)), role = Some("*")),
        Resource(name = "disk", `type` = Value.Type.SCALAR, scalar = Some(Value.Scalar(1024.0)), role = Some("*"))))

    val inputEvents = List(
      SchedulerLogicInputEvent.MarathonStateUpdate(
        Seq(
          StateTransition.RunSpecUpdated(
            sampleRunSpec.ref,
            Some(sampleRunSpec)),
          StateTransition.InstanceUpdated(
            sampleInstance.instanceId,
            Some(sampleInstance)))),
      SchedulerLogicInputEvent.MesosOffer(offer))

    val results: Seq[SchedulerLogic.Effect] = Source(inputEvents)
      .via(SchedulerLogic.eventProcesorFlow(clock))
      .runWith(Sink.seq)
      .futureValue

    results(0) shouldBe SchedulerLogic.Effect.WantOffers(instanceId)

    val offerResponse = inside(results(1)) {
      case r: SchedulerLogic.Effect.OfferResponse =>
        r.remainingOffer.id shouldBe offer.id
        r
    }

    val launch = inside(offerResponse.operations.flatMap(_.launch)) {
      case Seq(launch) =>
        launch
    }

    inside(launch.taskInfos) {
      case Seq(taskInfo) =>
        taskInfo.command.flatMap(_.value) shouldBe Some(sampleRunSpec.command)
        taskInfo.resources.toSet shouldBe Set(
          Resource(name = "cpus", `type` = Value.Type.SCALAR, scalar = Some(Value.Scalar(sampleRunSpec.cpus)), role = Some("*")),
          Resource(name = "mem", `type` = Value.Type.SCALAR, scalar = Some(Value.Scalar(sampleRunSpec.mem)), role = Some("*")))
    }

    val taskUpdate = inside(results(2)) {
      case t: SchedulerLogic.Effect.TaskUpdate =>
        t
    }

    taskUpdate.taskId shouldBe sampleInstance.mesosTaskId
    inside(taskUpdate.newState) {
      case Some(mesosTask) =>
        inside(mesosTask.phase) {
          case MesosTask.Phase.Launching(timestamp) =>
            timestamp shouldBe clock.instant()
        }
    }

    results(3) shouldBe SchedulerLogic.Effect.NoWantOffers(sampleInstance.instanceId)
  }
}
