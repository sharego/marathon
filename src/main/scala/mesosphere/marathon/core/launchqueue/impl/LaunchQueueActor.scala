package mesosphere.marathon
package core.launchqueue.impl

import akka.Done
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.Instance.InstanceState
import mesosphere.marathon.core.instance.update.{InstanceChange, InstanceUpdateOperation}
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfo
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{PathId, RunSpec, Timestamp}

import scala.async.Async.{async, await}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

private[launchqueue] object LaunchQueueActor {
  def props(
    config: LaunchQueueConfig,
    offerMatcherStatisticsActor: ActorRef,
    instanceTracker: InstanceTracker,
    runSpecActorProps: RunSpec => Props): Props = {
    Props(new LaunchQueueActor(config, offerMatcherStatisticsActor, instanceTracker, runSpecActorProps))
  }

  case class FullCount(appId: PathId)
}

/**
  * An actor-based implementation of the LaunchQueue interface.
  *
  * The methods of that interface are translated to messages in the [[LaunchQueueDelegate]] implementation.
  */
private[impl] class LaunchQueueActor(
    launchQueueConfig: LaunchQueueConfig,
    offerMatchStatisticsActor: ActorRef,
    instanceTracker: InstanceTracker,
    runSpecActorProps: RunSpec => Props) extends Actor with StrictLogging {
  import LaunchQueueDelegate._

  /** Currently active actors by pathId. */
  var launchers = Map.empty[PathId, ActorRef]
  /** Maps actorRefs to the PathId they handle. */
  var launcherRefs = Map.empty[ActorRef, PathId]

  /** Serial ID to ensure unique names for children actors. */
  var childSerial = 0

  // See [[receiveHandlePurging]]
  /** A message with a sender for later processing. */
  case class DeferredMessage(sender: ActorRef, message: Any)
  /** PathIds for which the actors have been currently suspended because we wait for their termination. */
  var suspendedLauncherPathIds = Set.empty[PathId]
  /** ActorRefs of the actors have been currently suspended because we wait for their termination. */
  var suspendedLaunchersMessages = Map.empty[ActorRef, Vector[DeferredMessage]].withDefaultValue(Vector.empty)

  /** The timeout for asking any children of this actor. */
  implicit val askTimeout: Timeout = launchQueueConfig.launchQueueRequestTimeout().milliseconds

  override def receive: Receive = LoggingReceive {
    Seq(
      receiveHandlePurging,
      receiveInstanceUpdateToSuspendedActor,
      receiveMessagesToSuspendedActor,
      receiveInstanceUpdate,
      receiveHandleNormalCommands
    ).reduce(_.orElse[Any, Unit](_))
  }

  /**
    * Handles purging of an actor.
    *
    * Terminating an actor with a PoisonPill is not instantaneous. It will still process
    * all prior messages. While waiting for the termination of the actor, we might
    * receive further messages to the actor which would potentially lead to recreating it.
    *
    * Thus, we mark the actor as suspended and save all messages which would normally
    * have been sent to this actor. When we receive confirmation of the actor's death (Terminated),
    * we will replay these messages to ourselves with the correct sender.
    */
  @SuppressWarnings(Array("all")) // async/await
  private[this] def receiveHandlePurging: Receive = {
    case Purge(runSpecId) =>
      logger.info(s"Processing purge request for $runSpecId")
      launchers.get(runSpecId) match {
        case Some(actorRef) =>
          val deferredMessages: Vector[DeferredMessage] =
            suspendedLaunchersMessages(actorRef) :+ DeferredMessage(sender(), ConfirmPurge(runSpecId))
          suspendedLaunchersMessages += actorRef -> deferredMessages
          suspendedLauncherPathIds += runSpecId
          actorRef ! TaskLauncherActor.Stop
        case None => sender() ! Done
      }

    case ConfirmPurge(runSpecId) =>
      import context.dispatcher

      async {
        logger.info("Removing scheduled instances")
        val scheduledInstances = await(instanceTracker.specInstances(runSpecId)).filter(_.isScheduled)
        val expungingScheduledInstances = Future.sequence(scheduledInstances.map { i => instanceTracker.forceExpunge(i.instanceId) })
        val dones = await(expungingScheduledInstances)
        Done
      }.pipeTo(sender())

    case Terminated(actorRef) =>
      launcherRefs.get(actorRef) match {
        case Some(pathId) =>
          launcherRefs -= actorRef
          launchers -= pathId

          suspendedLaunchersMessages.get(actorRef) match {
            case None =>
              logger.warn(s"Got unexpected terminated for runSpec $pathId: $actorRef")
            case Some(deferredMessages) =>
              deferredMessages.foreach(msg => self.tell(msg.message, msg.sender))

              suspendedLauncherPathIds -= pathId
              suspendedLaunchersMessages -= actorRef
          }
        case None =>
          logger.warn(s"Don't know anything about terminated actor: $actorRef")
      }
  }

  private[this] def receiveInstanceUpdateToSuspendedActor: Receive = {
    case update: InstanceChange if suspendedLauncherPathIds(update.runSpecId) =>
      // Do not defer. If an AppTaskLauncherActor restarts, it retrieves a new task list.
      // If we defer this, there is a potential deadlock (resolved by timeout):
      //   * AppTaskLauncher waits for in-flight tasks
      //   * TaskOp gets processed and one of the update steps calls this here
      //   * ... blocked until timeout ...
      //   * The task launch notification (that the AppTaskLauncherActor waits for) gets sent to the actor
      sender() ! Done
  }

  private[this] def receiveMessagesToSuspendedActor: Receive = {
    case msg @ Count(appId) if suspendedLauncherPathIds(appId) =>
      // Deferring this would also block List.
      sender() ! None

    case msg @ Add(app, count) if suspendedLauncherPathIds(app.id) =>
      deferMessageToSuspendedActor(msg, app.id)

    case msg @ RateLimiterActor.DelayUpdate(app, _) if suspendedLauncherPathIds(app.id) =>
      deferMessageToSuspendedActor(msg, app.id)
  }

  private[this] def deferMessageToSuspendedActor(msg: Any, appId: PathId): Unit = {
    val actorRef = launchers(appId)
    val deferredMessages: Vector[DeferredMessage] =
      suspendedLaunchersMessages(actorRef) :+ DeferredMessage(sender(), msg)
    suspendedLaunchersMessages += actorRef -> deferredMessages
  }

  private[this] def receiveInstanceUpdate: Receive = {
    case update: InstanceChange =>
      launchers.get(update.runSpecId) match {
        case Some(actorRef) => actorRef.forward(update)
        case None => sender() ! Done
      }
  }

  private[this] def list(): Future[Seq[QueuedInstanceInfo]] = {
    import context.dispatcher
    val scatter = launchers
      .keys
      .map(appId => (self ? Count(appId)).mapTo[Option[QueuedInstanceInfo]])
    Future.sequence(scatter).map(_.flatten.to[Seq])
  }

  @SuppressWarnings(Array("all")) // async/await
  private[this] def receiveHandleNormalCommands: Receive = {
    case List =>
      import context.dispatcher
      val to = sender()
      val infos: Future[Seq[QueuedInstanceInfo]] = list()
      infos.pipeTo(to)

    case ListWithStatistics =>
      import context.dispatcher
      val to = sender()
      list().map(OfferMatchStatisticsActor.SendStatistics(to, _)).pipeTo(offerMatchStatisticsActor)

    case Count(appId) =>
      import context.dispatcher
      launchers.get(appId) match {
        case Some(actorRef) =>
          val eventualCount: Future[QueuedInstanceInfo] =
            (actorRef ? TaskLauncherActor.GetCount).mapTo[QueuedInstanceInfo]
          eventualCount.map(Some(_)).pipeTo(sender())
        case None => sender() ! None
      }

    case Add(runSpec, count) =>
      import context.dispatcher

      async {
        val existingReserved = await(instanceTracker.specInstances(runSpec.id))
          .filter(i => i.isReserved && i.tasksMap.values.forall(_.status.condition.isTerminal))
          .take(count)
          .map(_.copy(state = InstanceState(Condition.Scheduled, Timestamp.now(), None, None), runSpecVersion = runSpec.version, unreachableStrategy = runSpec.unreachableStrategy))
          .map(InstanceUpdateOperation.RescheduleReserved)
        val instancesToSchedule = existingReserved.length.until(count).map { _ => Instance.Scheduled(runSpec, Instance.Id.forRunSpec(runSpec.id)) }
        if (instancesToSchedule.nonEmpty) {
          val scheduled = await(instanceTracker.schedule(instancesToSchedule))
        }
        val relaunched = await(Future.sequence(existingReserved.map(instanceTracker.process)))

        // Trigger TaskLaunchActor creation and sync with instance tracker.
        val actorRef = launchers.getOrElse(runSpec.id, createAppTaskLauncher(runSpec))
        val info = await((actorRef ? TaskLauncherActor.Sync(runSpec)).mapTo[QueuedInstanceInfo])
        Done
      }.pipeTo(sender())

    case msg @ RateLimiterActor.DelayUpdate(app, _) =>
      launchers.get(app.id).foreach(_.forward(msg))
  }

  private[this] def createAppTaskLauncher(app: RunSpec): ActorRef = {
    val actorRef = context.actorOf(runSpecActorProps(app), s"$childSerial-${app.id.safePath}")
    childSerial += 1
    launchers += app.id -> actorRef
    launcherRefs += actorRef -> app.id
    context.watch(actorRef)
    actorRef
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(e) =>
      // We periodically check if scaling is needed, so we should eventually recover.
      // TODO: Spead up recovery, e.g. by initiating a scale check.
      // Just restarting an AppTaskLauncherActor will potentially lead to starting too many tasks.
      Stop
    case m: Any => SupervisorStrategy.defaultDecider(m)
  }
}
