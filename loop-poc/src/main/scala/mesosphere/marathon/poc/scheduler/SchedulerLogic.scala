package mesosphere.marathon
package poc.scheduler

import akka.NotUsed
import akka.stream.scaladsl.Flow
import java.time.{ Clock, Instant }
import java.util.UUID
import mesosphere.marathon.poc.repository.{ StateFrame, StateTransition }
import mesosphere.marathon.poc.repository.MarathonState
import mesosphere.marathon.poc.state.{ Instance, RunSpec }
import org.apache.mesos.v1.mesos
import monocle.macros.syntax.lens._
import scala.annotation.tailrec

case class MesosTaskId(
    appName: String,
    instanceId: UUID,
    incarnation: Long) {
  def asString: String = s"${appName}#${instanceId}#${incarnation}"
}

object MesosTaskId {
  private[scheduler] val emptyUUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

  private def parseMesosTaskId(mesosTaskId: String): Option[(String, UUID, Long)] =
    mesosTaskId.split("#") match {
      case Array(name, uuid, incarnation) =>
        Some((name, UUID.fromString(uuid), java.lang.Long.parseLong(incarnation)))
      case _ =>
        None
    }

  def apply(taskId: String): MesosTaskId = parseMesosTaskId(taskId) match {
    case Some((appName, instanceId, incarnation)) => MesosTaskId(appName, instanceId, incarnation)
    case None =>
      // shouldn't really get here
      MesosTaskId("???", emptyUUID, 0L)
  }
}

case class MesosTask(
    taskId: MesosTaskId,
    agentId: String,
    phase: MesosTask.Phase) {
}

object MesosTask {

  def considerRunning(phase: Phase) = phase match {
    case _: Phase.Launching | _: Phase.Running => true
    case _: Phase.Killing | _: Phase.Terminal => false
  }
  def considerStopped(phase: Phase) = phase match {
    case _: Phase.Launching | _: Phase.Running => false
    case _: Phase.Killing | _: Phase.Terminal => true
  }

  def apply(mesosTask: mesos.TaskStatus): Option[MesosTask] =
    mesosTask.agentId.value map { agentId =>
      MesosTask(
        MesosTaskId(mesosTask.taskId.value),
        agentId.value,
        Phase(mesosTask))
    }

  /**
    * LOL don't read this too closely. just needed something quick to model task life-cycle
    */
  sealed trait Phase
  object Phase {
    def apply(taskStatus: mesos.TaskStatus): Phase = {
      import mesos.TaskState._
      val timestamp = Instant.ofEpochMilli((taskStatus.timestamp.getOrElse(0.0) * 1000).toLong)

      taskStatus.state match {
        case TASK_STAGING | TASK_STARTING | TASK_RUNNING
          | TASK_UNREACHABLE | TASK_LOST | TASK_UNKNOWN =>
          Running(taskStatus)
        case TASK_KILLING =>
          Killing(timestamp, taskStatus)
        case TASK_FINISHED | TASK_FAILED | TASK_KILLED | TASK_ERROR | TASK_DROPPED | TASK_GONE | TASK_GONE_BY_OPERATOR =>
          Terminal(timestamp, taskStatus)
        case Unrecognized(_) =>
          // ¯\_(ツ)_/¯
          ???
      }
    }

    case class Launching(timestamp: Instant) extends Phase
    /**
      * We have received a MesosStatus for this task.
      */
    case class Running(
        status: mesos.TaskStatus) extends Phase

    /** We're killing this task */
    case class Killing(
        lastkilledAt: Instant,
        status: mesos.TaskStatus) extends Phase

    case class Terminal(
        timestamp: Instant,
        status: mesos.TaskStatus) extends Phase
  }
}

case class MesosAgent(
    id: String,
    hostname: String)

case class MesosState(
    agents: Map[String, MesosAgent],
    tasks: Map[MesosTaskId, MesosTask])

object MesosState {
  def empty = MesosState(Map.empty, Map.empty)
}

sealed trait SchedulerLogicInputEvent
object SchedulerLogicInputEvent {
  case class MarathonStateUpdate(updates: Seq[StateTransition]) extends SchedulerLogicInputEvent
  case class MesosOffer(offer: mesos.Offer) extends SchedulerLogicInputEvent
  case class MesosTaskStatus(task: MesosTask) extends SchedulerLogicInputEvent
  case class QueryFrame(requestId: UUID) extends SchedulerLogicInputEvent
}

case class SchedulerFrame(
    state: MarathonState,
    mesosState: MesosState)

object SchedulerFrame {
  def empty = SchedulerFrame(state = MarathonState.empty, MesosState.empty)
}

object SchedulerLogic {
  sealed trait Effect
  sealed trait MesosEffect extends Effect
  object Effect {
    sealed trait OfferSignal extends MesosEffect
    /**
      * Indicate that an instance wants offers (so it can launch)
      *
      * for multi-role could include the role here so downstream Mesos can update subscription accordingly
      */
    case class WantOffers(instanceId: UUID) extends OfferSignal
    case class NoWantOffers(instanceId: UUID) extends OfferSignal

    /**
      * Indicate to Mesos that a task should be killed
      */
    case class KillTask(taskId: MesosTaskId, agent: Option[String]) extends MesosEffect

    /**
      * Indicate that the task is to be forgotten.
      */
    case class TaskUpdate(taskId: MesosTaskId, newState: Option[MesosTask]) extends Effect
    // Maybe move this to the default orchestator?
    case class BumpIncarnation(instanceId: UUID, incarnation: Long) extends Effect

    case class EmitState(requestId: UUID, frame: SchedulerFrame) extends Effect

    case class OfferResponse(
        offerId: mesos.OfferID,
        remainingOffer: mesos.Offer,
        operations: Seq[mesos.Offer.Operation]
    ) extends MesosEffect
  }

  def computeEffect(marathonInstance: Option[Instance], mesosTask: Option[MesosTask])(clock: Clock): Seq[Effect] = {
    def killExpunge(taskId: MesosTaskId, agentId: Option[String]): List[Effect] =
      List(
        Effect.KillTask(taskId, agentId),
        Effect.TaskUpdate(taskId, None))

    /**
      * Returns effects to kill a task and update the status to with a provisionally killing status
      *
      *
      */
    def kill(task: MesosTask): List[Effect] =
      task.phase match {
        case MesosTask.Phase.Running(status) =>
          List(
            Effect.KillTask(task.taskId, Some(task.agentId)),
            Effect.TaskUpdate(
              task.taskId,
              Some(task.copy(phase = MesosTask.Phase.Killing(clock.instant(), status)))))
        case other =>
          killExpunge(task.taskId, Some(task.agentId))
      }

    (marathonInstance, mesosTask) match {
      case (Some(instance), None) =>
        Seq(Effect.WantOffers(instance.instanceId))
      case (Some(instance), Some(task)) if task.taskId.incarnation < instance.incarnation =>
        kill(task)
      case (None, Some(task)) =>
        killExpunge(task.taskId, Some(task.agentId))
      case (Some(instance), Some(task)) if task.taskId.incarnation == instance.incarnation =>
        (instance.goal, MesosTask.considerRunning(task.phase)) match {
          case (Instance.Goal.Running, true) =>
            Nil
          case (Instance.Goal.Running, false) =>
            Seq(Effect.BumpIncarnation(instance.instanceId, instance.incarnation + 1))
          case (Instance.Goal.Stopped, true) =>
            kill(task)
          case (Instance.Goal.Stopped, false) =>
            Nil
        }
      case (None, None) =>
        throw new RuntimeException("we should not get here")
      case (Some(instance), Some(task)) if task.taskId.incarnation > instance.incarnation =>
        /* We should never get here. */

        if (MesosTask.considerRunning(task.phase))
          Effect.BumpIncarnation(instance.instanceId, task.taskId.incarnation + 1) ::
            killExpunge(task.taskId, Some(task.agentId))
        else
          Nil
    }
  }

  /**
    * Returns the effects currently necessary to advance towards the goal
    */
  def computeEffects(marathonState: MarathonState, mesosState: MesosState, affectedInstances: Seq[UUID], affectedTasks: Seq[MesosTaskId])(clock: Clock): (List[Effect], MesosState) = {
    val instanceEffects = affectedInstances.distinct.iterator.
      flatMap(marathonState.instances.instances.get).
      flatMap { instance =>
        computeEffect(Some(instance), mesosState.tasks.get(instance.mesosTaskId))(clock)
      }
    val taskEffects = affectedTasks.distinct.iterator.
      flatMap(mesosState.tasks.get).
      flatMap { task =>
        computeEffect(marathonState.instances.instances.get(task.taskId.instanceId), Some(task))(clock)
      }

    val effects = (instanceEffects ++ taskEffects).toList

    (effects, applyTaskUpdates(mesosState, effects.collect { case u: Effect.TaskUpdate => u }))
  }

  def applyTaskUpdates(mesosState: MesosState, effects: Seq[Effect.TaskUpdate]): MesosState = {
    mesosState.lens(_.tasks).modify { tasks =>
      effects.foldLeft(tasks) {
        case (tasks, Effect.TaskUpdate(taskId, None)) =>
          tasks - taskId
        case (tasks, Effect.TaskUpdate(taskId, Some(task))) =>
          tasks.updated(taskId, task)
      }
    }
  }

  case class RejectReason(str: String)

  /**
    * Silly match function
    *
    * Return:
    *   Right((remainingOffer, matchedResources)) | Left(rejectReason)
    */
  def matchOffers(offer: mesos.Offer, runSpec: RunSpec): Either[RejectReason, (mesos.Offer, Seq[mesos.Resource])] = {
    def consumeScalar(resource: mesos.Resource, amount: Double): (mesos.Resource, mesos.Resource) = {
      val remaining = resource.lens(_.scalar).modify {
        case Some(scalar) => Some(mesos.Value.Scalar(scalar.value - amount))
        case None => None // shouldn't get here
      }
      val consumed = resource.lens(_.scalar).modify {
        case Some(scalar) => Some(mesos.Value.Scalar(amount))
        case None => None // shouldn't get here
      }

      (remaining, consumed)
    }

    case class ResourceMatcher(description: String, fn: PartialFunction[mesos.Resource, (mesos.Resource, mesos.Resource)]) {
    }

    val cpusMatcher = ResourceMatcher(s"cpus ${runSpec.cpus}", {
      case cpus if cpus.name == "cpus" && cpus.scalar.forall(_.value >= runSpec.cpus) =>
        consumeScalar(cpus, runSpec.cpus)
    })
    val memMatcher = ResourceMatcher(s"mem ${runSpec.mem}", {
      case mem if mem.name == "mem" && mem.scalar.forall(_.value >= runSpec.mem) =>
        consumeScalar(mem, runSpec.mem)
    })

    @tailrec def runMatches(matchers: List[ResourceMatcher], resources: List[mesos.Resource], matched: List[mesos.Resource]): Either[RejectReason, (List[mesos.Resource], List[mesos.Resource])] = {
      matchers match {
        case Nil =>
          Right((resources, matched))
        case m :: restMatchers =>
          val matchIndex = resources.indexWhere { r => m.fn.isDefinedAt(r) }
          if (matchIndex == -1)
            Left(RejectReason(s"Not all resources were satisfied: ${m.description}"))
          else {
            val (remaining, thisMatch) = m.fn(resources(matchIndex))
            runMatches(
              restMatchers,
              resources.updated(matchIndex, remaining),
              thisMatch :: matched)
          }
      }
    }

    runMatches(List(cpusMatcher, memMatcher), offer.resources.toList, Nil).right map {
      case (remaining, matched) =>
        (offer.copy(resources = remaining), matched)
    }
  }

  case class OfferMatch(resources: Seq[mesos.Resource], instance: Instance, runSpec: RunSpec)
  @tailrec final def doMatch(instances: List[Instance], offer: mesos.Offer, state: MarathonState, matches: List[OfferMatch]): (mesos.Offer, List[OfferMatch]) = {
    instances match {
      case Nil =>
        (offer, matches)
      case i :: restInstances =>
        val runSpec = state.rootGroup.get(i.runSpec).get
        matchOffers(offer, runSpec) match {
          case Left(reject) =>
            println(reject)
            doMatch(restInstances, offer, state, matches)
          case Right((remainingOffer, resources)) =>
            // let's match lol
            doMatch(restInstances, remainingOffer, state, OfferMatch(resources, i, runSpec) :: matches)
        }
    }
  }

  def eventProcesorFlow(clock: Clock = Clock.systemUTC()): Flow[SchedulerLogicInputEvent, Effect, NotUsed] =
    Flow[SchedulerLogicInputEvent].statefulMapConcat { () =>
      var frame = SchedulerFrame.empty
      val wantingOffers = collection.mutable.Set.empty[UUID]

      { inputEvent =>

        val effects = inputEvent match {
          case SchedulerLogicInputEvent.MesosTaskStatus(task) =>
            val taskUpdates = if (frame.mesosState.tasks.get(task.taskId).contains(task)) {
              Nil
            } else {
              Seq(Effect.TaskUpdate(task.taskId, Some(task)))
            }

            val mesosStateWithTaskUpdates = applyTaskUpdates(frame.mesosState, taskUpdates)

            val (effects, finalMesosState) = computeEffects(
              frame.state, mesosStateWithTaskUpdates, Nil, taskUpdates.map(_.taskId))(clock)

            frame = frame.copy(
              mesosState = finalMesosState)

            taskUpdates ++ effects

          case SchedulerLogicInputEvent.MesosOffer(offer) =>
            // if any instances are removed, we need to remove them from wantingOffers also.
            wantingOffers.retain(frame.state.instances.instances.contains(_))

            // match offers for pending tasks
            doMatch(
              wantingOffers.flatMap(frame.state.instances.instances.get)(collection.breakOut),
              offer,
              frame.state,
              Nil) match {
              case (_, Nil) =>
                Seq(Effect.OfferResponse(offer.id, offer, Nil))
              case (remaining, matches) =>
                val taskInfos = matches.map {
                  case OfferMatch(resources, instance, runSpec) =>
                    mesos.TaskInfo(
                      name = instance.mesosTaskId.asString,
                      taskId = mesos.TaskID(instance.mesosTaskId.asString),
                      agentId = offer.agentId,
                      resources= resources,
                      command = Some(mesos.CommandInfo(value = Some(runSpec.command), shell = Some(true))))
                }

                val launchOperation = Effect.OfferResponse(
                  offer.id,
                  remaining,
                  Seq(
                    mesos.Offer.Operation(
                      launch = Some(mesos.Offer.Operation.Launch(taskInfos)))))

                val noWantOffers = matches.map { m => Effect.NoWantOffers(m.instance.instanceId) }
                val taskUpdates = matches.map { m =>
                  Effect.TaskUpdate(m.instance.mesosTaskId,
                    Some(
                      MesosTask(
                        taskId = m.instance.mesosTaskId,
                        agentId = offer.agentId.value,
                        phase = MesosTask.Phase.Launching(clock.instant())))),
                }

                frame = frame.copy(mesosState = applyTaskUpdates(frame.mesosState, taskUpdates))

                launchOperation :: (taskUpdates ++ noWantOffers)
            }

          case SchedulerLogicInputEvent.QueryFrame(requestId) =>
            Seq(Effect.EmitState(requestId, frame))

          case SchedulerLogicInputEvent.MarathonStateUpdate(updates) =>
            val nextMarathonState = StateTransition.applyTransitions(frame.state, updates)
            val (effects, finalMesosState) = computeEffects(nextMarathonState, frame.mesosState, StateTransition.affectedInstanceIds(updates), Nil)(clock)

            frame = frame.copy(state = nextMarathonState, mesosState = finalMesosState)

            effects
        }

        // update wantingOffers
        effects.foreach {
          case e: Effect.NoWantOffers =>
            wantingOffers -= e.instanceId
          case e: Effect.WantOffers =>
            wantingOffers += e.instanceId
          /* case e: Effect.LaunchTask(instanceId, _) => wantingOffers -= instanceId */
          case _ =>
        }

        effects
      }
    }

  /**
    * Given Mesos
    */
  def shouldRevive(frame: StateFrame) = {

  }
}
