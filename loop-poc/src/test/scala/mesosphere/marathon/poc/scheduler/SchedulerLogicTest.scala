package mesosphere.marathon
package poc.scheduler

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import java.util.UUID
import mesosphere.marathon.poc.repository.StateTransition
import mesosphere.marathon.poc.state.{ Instance, RunSpec, RunSpecRef }
import mesosphere.marathon.test.SettableClock
import org.apache.mesos.v1.mesos.{ AgentID, TaskID, TaskState, TaskStatus }
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
    val (input, result) = Source.queue[SchedulerLogicInputEvent](16, OverflowStrategy.fail)
      .via(SchedulerLogic.eventProcesorFlow())
      .toMat(Sink.queue())(Keep.both)
      .run

    input.offer(SchedulerLogicInputEvent.MarathonStateUpdate(
      Seq(
        StateTransition.RunSpecUpdated(
          sampleRunSpec.ref,
          Some(sampleRunSpec)),
        StateTransition.InstanceUpdated(
          sampleInstance.instanceId,
          Some(sampleInstance)))))

    result.pull().futureValue shouldBe Some(SchedulerLogic.Effect.WantOffers(instanceId))
    result
  }

  "it kills unknown running tasks and expunges the task" in {
    val clock = new SettableClock
    val (input, result) = Source.queue[SchedulerLogicInputEvent](16, OverflowStrategy.fail)
      .via(SchedulerLogic.eventProcesorFlow(clock))
      .toMat(Sink.queue())(Keep.both)
      .run

    val mesosTaskId = MesosTaskId("lol", MesosTaskId.emptyUUID, 1L)
    val agentId = "lolol"
    val status = TaskStatus(
      TaskID(mesosTaskId.asString),
      TaskState.TASK_RUNNING,
      timestamp = Some(0.0),
      agentId = Some(AgentID(agentId)))
    val Some(task) = MesosTask.apply(status)

    input.offer(SchedulerLogicInputEvent.MesosTaskStatus(task))

    inside(result.pull().futureValue) {
      case Some(SchedulerLogic.Effect.TaskUpdate(taskId, newState)) =>
        taskId shouldBe mesosTaskId
    }
    inside(result.pull().futureValue) {
      case Some(SchedulerLogic.Effect.KillTask(taskId, Some(agentId))) =>
        taskId shouldBe mesosTaskId
        agentId shouldBe agentId
    }

    inside(result.pull().futureValue) {
      case Some(SchedulerLogic.Effect.TaskUpdate(taskId, None)) =>
    }
  }
}
