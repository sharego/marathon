package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor.{OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.testkit.{TestActorRef, TestProbe}
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.condition.Condition.{Failed, Running}
import mesosphere.marathon.core.event.{DeploymentStatus, _}
import mesosphere.marathon.core.health.MesosCommandHealthCheck
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launcher.impl.LaunchQueueTestHelper
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.{AppDefinition, Command}
import org.scalatest.concurrent.Eventually

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

class TaskStartActorTest extends AkkaUnitTest with Eventually {
  "TaskStartActor" should {
    for (
      (counts, description) <- Seq(
        None -> "with no item in queue",
        Some(LaunchQueueTestHelper.zeroCounts) -> "with zero count queue item"
      )
    ) {
      s"Start success $description" in {
        val f = new Fixture
        val promise = Promise[Unit]()
        val app = AppDefinition("/myApp".toPath, instances = 5)

        f.launchQueue.get(app.id) returns Future.successful(counts)
        f.taskTracker.instancesToScheduleCount(app.id, 5) returns Future.successful(5)
        val ref = f.startActor(app, app.instances, promise)
        watch(ref)

        eventually { verify(f.launchQueue, atLeastOnce).add(app, app.instances) }

        for (i <- 0 until app.instances)
          system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

        promise.future.futureValue should be(())

        expectTerminated(ref)
      }
    }

    "Start success with one task left to launch" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 5)

      f.taskTracker.instancesToScheduleCount(app.id, 5) returns Future.successful(4)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      eventually { verify(f.launchQueue, atLeastOnce).add(app, app.instances - 1) }

      for (i <- 0 until (app.instances - 1))
        system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start success with existing task in launch queue" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 5)

      f.launchQueue.get(app.id) returns Future.successful(None)
      f.taskTracker.instancesToScheduleCount(app.id, 5) returns Future.successful(4)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      eventually { verify(f.launchQueue, atLeastOnce).add(app, app.instances - 1) }

      for (i <- 0 until (app.instances - 1))
        system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start success with no instances to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 0)
      f.launchQueue.get(app.id) returns Future.successful(None)
      f.taskTracker.instancesToScheduleCount(app.id, 0) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start with health checks" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition(
        "/myApp".toPath,
        instances = 5,
        healthChecks = Set(MesosCommandHealthCheck(command = Command("true")))
      )
      f.launchQueue.get(app.id) returns Future.successful(None)
      f.taskTracker.instancesToScheduleCount(app.id, 5) returns Future.successful(5)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      eventually { verify(f.launchQueue, atLeastOnce).add(app, app.instances) }

      for (i <- 0 until app.instances)
        system.eventStream.publish(f.healthChange(app, Instance.Id.forRunSpec(app.id), healthy = true))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Start with health checks with no instances to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition(
        "/myApp".toPath,
        instances = 0,
        healthChecks = Set(MesosCommandHealthCheck(command = Command("true")))
      )
      f.launchQueue.get(app.id) returns Future.successful(None)
      f.taskTracker.instancesToScheduleCount(app.id, 0) returns Future.successful(0)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }

    "Task fails to start" in {
      val f = new Fixture
      val promise = Promise[Unit]()
      val app = AppDefinition("/myApp".toPath, instances = 1)

      f.launchQueue.get(app.id) returns Future.successful(None)
      f.taskTracker.instancesToScheduleCount(app.id, 1) returns Future.successful(1)

      val ref = f.startActor(app, app.instances, promise)
      watch(ref)

      eventually { verify(f.launchQueue, atLeastOnce).add(app, app.instances) }

      system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Failed))

      eventually { verify(f.launchQueue, atLeastOnce).add(app, 1) }

      for (i <- 0 until app.instances)
        system.eventStream.publish(f.instanceChange(app, Instance.Id.forRunSpec(app.id), Running))

      promise.future.futureValue should be(())

      expectTerminated(ref)
    }
  }

  class Fixture {

    val scheduler: SchedulerActions = mock[SchedulerActions]
    val launchQueue: LaunchQueue = mock[LaunchQueue]
    val taskTracker: InstanceTracker = mock[InstanceTracker]
    val deploymentManager = TestProbe()
    val status: DeploymentStatus = mock[DeploymentStatus]
    val readinessCheckExecutor: ReadinessCheckExecutor = mock[ReadinessCheckExecutor]

    launchQueue.add(any, any) returns Future.successful(Done)

    def instanceChange(app: AppDefinition, id: Instance.Id, condition: Condition): InstanceChanged = {
      val instance: Instance = mock[Instance]
      instance.instanceId returns id
      InstanceChanged(id, app.version, app.id, condition, instance)
    }

    def healthChange(app: AppDefinition, id: Instance.Id, healthy: Boolean): InstanceHealthChanged = {
      InstanceHealthChanged(id, app.version, app.id, Some(healthy))
    }

    def startActor(app: AppDefinition, scaleTo: Int, promise: Promise[Unit]): TestActorRef[TaskStartActor] =
      TestActorRef(childSupervisor(TaskStartActor.props(
        deploymentManager.ref, status, scheduler, launchQueue, taskTracker, system.eventStream, readinessCheckExecutor,
        app, scaleTo, promise), "Test-TaskStartActor"))

    // Prevents the TaskActor from restarting too many times (filling the log with exceptions) similar to how it's
    // parent actor (DeploymentActor) does it.
    def childSupervisor(props: Props, name: String): Props = {
      import scala.concurrent.duration._

      BackoffSupervisor.props(
        Backoff.onFailure(
          childProps = props,
          childName = name,
          minBackoff = 5.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
        ).withSupervisorStrategy(
          OneForOneStrategy() {
            case NonFatal(_) => SupervisorStrategy.Restart
            case _ => SupervisorStrategy.Escalate
          }
        ))
    }
  }
}
