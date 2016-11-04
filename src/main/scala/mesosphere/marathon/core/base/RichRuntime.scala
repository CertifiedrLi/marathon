package mesosphere.marathon.core.base

import java.util.{ Timer, TimerTask }

import akka.Done
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, _ }

/**
  * Add asyncExit method to Runtime.
  */
case class RichRuntime(runtime: Runtime) extends StrictLogging {

  /**
    * Exit this process in an async fashion.
    * First try exit regularly in the given timeout. If this does not exit in time, we halt the system.
    *
    * @param exitCode    the exit code to signal.
    * @param waitForExit the time to wait for a normal exit.
    * @return the Future of this operation.
    */
  def asyncExit(
    exitCode: Int = RichRuntime.FatalErrorSignal,
    waitForExit: FiniteDuration = RichRuntime.DefaultExitDelay)(implicit ec: ExecutionContext): Future[Done] = {
    val timer = new Timer()
    val promise = Promise[Done]()
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        logger.info("Halting JVM")
        promise.success(Done)
        if (sys.props.get("java.class.path").exists(_.contains("test-classes"))) {
          // do nothing, we're in a test and we can't guarantee we can block the exit
        } else {
          Runtime.getRuntime.halt(exitCode)
        }
        Runtime.getRuntime.halt(exitCode)
      }
    }, waitForExit.toMillis)
    Future(sys.exit(exitCode))
    promise.future
  }
}

object RichRuntime {
  val FatalErrorSignal = 137
  val DefaultExitDelay = 10.seconds
}
