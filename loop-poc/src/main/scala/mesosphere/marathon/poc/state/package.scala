package mesosphere.marathon
package poc

import java.util.UUID
import mesosphere.marathon.poc.scheduler.MesosTaskId

package state {
  /**
    * Represents some command to run
    */
  case class RunSpec(
      id: String,
      variant: Variant,
      version: String,
      command: String,
      cpus: Double = 1.0,
      mem: Double = 32.0) {
    def ref = RunSpecRef(id, variant)
  }

  case class RunSpecRef(
      id: String,
      variant: Variant) {
    override def toString(): String = s"${id}#${variant}"
  }

  case class Instance(
      instanceId: UUID,
      runSpec: RunSpecRef,
      incarnation: Long,
      goal: Instance.Goal
  ) {
    lazy val mesosTaskId = MesosTaskId(runSpec.id, instanceId, incarnation)
  }

  object Instance {
    sealed trait Goal
    object Goal {
      case object Running extends Goal
      case object Stopped extends Goal
    }

    def mesosIdFor(instance: Instance): MesosTaskId =
      MesosTaskId(instance.runSpec.id, instance.instanceId, instance.incarnation)
  }

  case class DeploymentPolicy(
      id: UUID,
      runSpec: RunSpecRef,
      placement: String,
      recovery: String,
      schedule: String)

  sealed trait UpgradePolicy
  case class RollingUpdate(
      overScale: Int,
      maxUnready: Int,
      instancesPerSecond: Option[Int]) extends UpgradePolicy

  case class Deployment(
      deploymentPolicyId: UUID,
      upgradePolicy: UpgradePolicy,
      targetInstanceCount: Int,
      runSpec: RunSpecRef)

}

package object state {
  type Path = String
  type Variant = String
}
