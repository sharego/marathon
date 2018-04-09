package mesosphere.mesos.examples.tracker

import mesosphere.mesos.examples.task
import org.apache.mesos.v1.mesos.TaskID

trait TrackerState {

  /**
    * @return a vector with all the tasks
    */
  def tasks: Vector[task.State]

  /**
    * @param taskID given a taskID
    * @return return it's state
    */
  def task(taskID: TaskID): task.State

  /**
    * @param name give a task name
    * @return
    */
  def task(name: String): task.State
}
