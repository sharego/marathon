package mesosphere.mesos.examples.task

import org.apache.mesos.v1.mesos.Resource

/**
  * A minimal version of marathon's RunSpec. Contains the command and the necessary resources.
  *
  * @param name task's pet name
  * @param cmd command to execute
  * @param resources list of necessary resources
  */
case class Spec(name: String,
                cmd: String,
                resources: Iterable[Resource])
