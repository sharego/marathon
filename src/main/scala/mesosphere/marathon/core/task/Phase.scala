package mesosphere.marathon
package core.task

import java.time.Instant
import org.apache.mesos.{Protos => MesosProtos}

/**
  * A superset of the MesosTask statuses that models transitional states
  */
sealed trait Phase
object Phase {
  def apply(taskStatus: MesosProtos.TaskStatus): Phase = {
    import MesosProtos.TaskState._
    val timeStampMillis = ((if (taskStatus.hasTimestamp) taskStatus.getTimestamp() else 0.0) * 1000).toLong
    val timestamp = Instant.ofEpochMilli(timeStampMillis)

    taskStatus.getState match {
      case TASK_STAGING | TASK_STARTING | TASK_RUNNING
         | TASK_UNREACHABLE | TASK_LOST | TASK_UNKNOWN =>
        Running(taskStatus)
      case TASK_KILLING =>
        Killing(timestamp, taskStatus)
      case TASK_FINISHED | TASK_FAILED | TASK_KILLED | TASK_ERROR | TASK_DROPPED | TASK_GONE | TASK_GONE_BY_OPERATOR =>
        Terminal(timestamp, taskStatus)
      case _ =>
        // ¯\_(ツ)_/¯
        ???
    }
  }

  /**
    * We have issues a command to launch a task, but have not yet received the timeback
    *
    * We have special logic for dropped launch requests in order to avoid those scenarios from following the unreachable
    * task logic, so we need to know at which point do we apply that logic.
    *
    * @param timestamp The time at which we sent a request to launch a task.
    */
  case class Launching(timestamp: Instant) extends Phase

  /**
    * We have received a MesosStatus for this task.
    */
  case class Running(
    status: MesosProtos.TaskStatus) extends Phase

  /** We're killing this task */
  case class Killing(
    lastkilledAt: Instant,
    status: MesosProtos.TaskStatus) extends Phase

  case class Terminal(
    timestamp: Instant,
    status: MesosProtos.TaskStatus) extends Phase
}
