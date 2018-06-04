package mesosphere.marathon
package core.group

import javax.inject.Provider
import akka.event.EventStream
import kamon.metric.instrument.Time
import mesosphere.marathon.api.GroupApiService
import mesosphere.marathon.core.group.impl.GroupManagerImpl
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.plugin.auth.Authorizer
import mesosphere.marathon.storage.repository.GroupRepository

import scala.concurrent.ExecutionContext

/**
  * Provides a [[GroupManager]] implementation.
  */
class GroupManagerModule(
    config: GroupManagerConfig,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository)(implicit ctx: ExecutionContext, eventStream: EventStream, authorizer: Authorizer) {

  val groupManager: GroupManager = {
    val groupManager = new GroupManagerImpl(config, None, groupRepo, scheduler)

    val startedAt = System.currentTimeMillis()
    Metrics.gauge(
      "marathon.uptime.duration",
      () => (System.currentTimeMillis() - startedAt) / 1000,
      Time.Seconds)

    groupManager
  }

  val groupService: GroupApiService = {
    new GroupApiService(groupManager)
  }
}
