package mesosphere.mesos.examples.task

import org.apache.mesos.v1.mesos
import org.apache.mesos.v1.mesos.{AgentInfo, TaskID}

/**
  * @param taskId task Id is unique for every task
  * @param spec task spec
  * @param target target state of the task representing user/orchestrator intent
  * @param current current task state derived from [[mesos.TaskState]]. It's an `Option` since it does not exists until
  *                the first status update from mesos
  * @param agent task agent information. It's an `Option` since it does not exists until the task is scheduled
  */
case class State(taskId: TaskID,
                 spec: Spec,
                 target: Target,
                 current: Option[Current],
                 agent: Option[AgentInfo])