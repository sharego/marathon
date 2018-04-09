package mesosphere.mesos.examples.task

import org.apache.mesos.v1.mesos
import org.apache.mesos.v1.mesos.TaskState._

sealed trait Status
sealed trait Target extends Status
sealed trait Current extends Status

/**
  * This is target condition of the task which represents the user intent (hence a verb as name). For the present status
  * of the task see [[Current]] trait.
  */
case object Target {

  case object Schedule extends Target

  case object Kill extends Target

}

/**
  * This is present condition of the task (hence the noun as name) which mirrors the mesos task state. For the target
  * status of the task see [[Target]] trait.
  */
case object Current {

  case object Staging extends Current

  case object Starting extends Current

  case object Running extends Current

  case object Killing extends Current

  case object Finished extends Current

  case object Failed extends Current

  case object Killed extends Current

  case object Error extends Current

  case object Lost extends Current

  case object Dropped extends Current

  case object Unreachable extends Current

  case object Gone extends Current

  case object GoneByOperator extends Current

  case object Unknown extends Current

  /**
    * 1-to-1 mapping of [[mesos.TaskState]] to the [[Current]] state.
    * @param state mesos task state
    * @return current state
    */
  def apply(state: mesos.TaskState): Current = state match {
    case TASK_STAGING => Current.Staging
    case TASK_STARTING => Current.Starting
    case TASK_RUNNING => Current.Running
    case TASK_KILLING => Current.Killing
    case TASK_FINISHED => Current.Finished
    case TASK_FAILED => Current.Failed
    case TASK_KILLED => Current.Killed
    case TASK_ERROR => Current.Error
    case TASK_LOST => Current.Lost
    case TASK_DROPPED => Current.Dropped
    case TASK_UNREACHABLE => Current.Unreachable
    case TASK_GONE => Current.Gone
    case TASK_GONE_BY_OPERATOR => Current.GoneByOperator
    case TASK_UNKNOWN => Current.Unknown
    case state => throw new IllegalArgumentException(s"Unknown mesos tasks state: $state")
  }
}
