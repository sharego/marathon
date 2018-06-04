package mesosphere.marathon
package metrics

import akka.Done
import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import akka.stream.scaladsl.Source
import java.time.{Clock, Duration}

import kamon.Kamon
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric.instrument.{Time, UnitOfMeasurement}
import kamon.metric.{Entity, SubscriptionFilter, instrument}
import kamon.util.MilliTimestamp

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Counter {
  def increment(): Counter
  def increment(times: Long): Counter
}

trait Gauge {
  def value(): Long
  def increment(by: Long = 1): Gauge
  def decrement(by: Long = 1): Gauge
}

trait SettableGauge extends Gauge {
  def setValue(value: Long): SettableGauge
}

trait Histogram {
  def record(value: Long): Histogram
  def record(value: Long, count: Long): Histogram
}

trait MinMaxCounter {
  def increment(): MinMaxCounter
  def increment(times: Long): MinMaxCounter
  def decrement(): MinMaxCounter
  def decrement(times: Long): MinMaxCounter
  def refreshValues(): MinMaxCounter
}

trait Timer {
  def apply[T](f: => Future[T]): Future[T]
  def forSource[T, M](f: => Source[T, M])(implicit clock: Clock = Clock.systemUTC): Source[T, M]
  def blocking[T](f: => T): T
  def update(value: Long): Timer
  def update(duration: FiniteDuration): Timer
}

object AcceptAllFilter extends SubscriptionFilter {
  override def accept(entity: Entity): Boolean = true
}

object Metrics {
  implicit class KamonCounter(val counter: instrument.Counter) extends Counter {
    override def increment(): KamonCounter = {
      counter.increment()
      this
    }
    override def increment(times: Long): KamonCounter = {
      counter.increment(times)
      this
    }
  }

  def counter(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Counter = {
    Kamon.metrics.counter(name(prefix, `class`, metricName), tags, unit)
  }

  def counter(metricName: String, tags: Map[String, String], unit: UnitOfMeasurement): Counter = {
    Kamon.metrics.counter(name(metricName), tags, unit)
  }

  def counter(metricName: String, unit: UnitOfMeasurement): Counter = {
    counter(metricName, Map.empty[String, String], unit)
  }

  def counter(metricName: String, tags: Map[String, String]): Counter = {
    counter(metricName, tags, UnitOfMeasurement.Unknown)
  }

  def counter(metricName: String): Counter = {
    counter(metricName, Map.empty[String, String], UnitOfMeasurement.Unknown)
  }

  private implicit class KamonGauge(val gauge: instrument.Gauge) extends Gauge {
    override def value(): Long = gauge.value()
    override def increment(by: Long): this.type = {
      gauge.increment(by)
      this
    }
    override def decrement(by: Long): this.type = {
      gauge.decrement(by)
      this
    }
  }

  def gauge(prefix: MetricPrefix, `class`: Class[_], metricName: String, currentValue: () => Long,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Gauge = {
    Kamon.metrics.gauge(name(prefix, `class`, metricName), tags, unit)(currentValue)
  }

  def gauge(metricName: String, currentValue: () => Long, tags: Map[String, String], unit: UnitOfMeasurement): Gauge = {
    Kamon.metrics.gauge(name(metricName), tags, unit)(currentValue)
  }

  def gauge(metricName: String, currentValue: () => Long, unit: UnitOfMeasurement): Gauge = {
    gauge(metricName, currentValue, Map.empty[String, String], unit)
  }

  def gauge(metricName: String, currentValue: () => Long, tags: Map[String, String]): Gauge = {
    gauge(metricName, currentValue, tags, UnitOfMeasurement.Unknown)
  }

  def gauge(metricName: String, currentValue: () => Long): Gauge = {
    gauge(metricName, currentValue, Map.empty[String, String], UnitOfMeasurement.Unknown)
  }

  def atomicGauge(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): SettableGauge = {
    AtomicGauge(name(prefix, `class`, metricName), unit, tags)
  }

  def atomicGauge(metricName: String, tags: Map[String, String], unit: UnitOfMeasurement): SettableGauge = {
    AtomicGauge(name(metricName), unit, tags)
  }

  def atomicGauge(metricName: String, unit: UnitOfMeasurement): SettableGauge = {
    atomicGauge(metricName, Map.empty[String, String], unit)
  }

  def atomicGauge(metricName: String, tags: Map[String, String]): SettableGauge = {
    atomicGauge(metricName, tags, UnitOfMeasurement.Unknown)
  }

  def atomicGauge(metricName: String): SettableGauge = {
    atomicGauge(metricName, Map.empty[String, String], UnitOfMeasurement.Unknown)
  }

  implicit class KamonHistogram(val histogram: instrument.Histogram) extends Histogram {
    override def record(value: Long): KamonHistogram = {
      histogram.record(value)
      this
    }

    override def record(value: Long, count: Long): KamonHistogram = {
      histogram.record(value, count)
      this
    }
  }

  def histogram(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): Histogram = {
    Kamon.metrics.histogram(name(prefix, `class`, metricName), tags, unit)
  }

  def histogram(metricName: String, tags: Map[String, String], unit: UnitOfMeasurement): Histogram = {
    Kamon.metrics.histogram(name(metricName), tags, unit)
  }

  def histogram(metricName: String, unit: UnitOfMeasurement): Histogram = {
    histogram(metricName, Map.empty[String, String], unit)
  }

  def histogram(metricName: String, tags: Map[String, String]): Histogram = {
    histogram(metricName, tags, UnitOfMeasurement.Unknown)
  }

  def histogram(metricName: String): Histogram = {
    histogram(metricName, Map.empty[String, String], UnitOfMeasurement.Unknown)
  }

  implicit class KamonMinMaxCounter(val counter: instrument.MinMaxCounter) extends MinMaxCounter {
    override def increment(): KamonMinMaxCounter = {
      counter.increment()
      this
    }

    override def increment(times: Long): KamonMinMaxCounter = {
      counter.increment(times)
      this
    }

    override def decrement(): KamonMinMaxCounter = {
      counter.decrement()
      this
    }

    override def decrement(times: Long): KamonMinMaxCounter = {
      counter.decrement(times)
      this
    }

    override def refreshValues(): KamonMinMaxCounter = {
      counter.refreshValues()
      this
    }
  }

  def minMaxCounter(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: UnitOfMeasurement = UnitOfMeasurement.Unknown): MinMaxCounter = {
    Kamon.metrics.minMaxCounter(name(prefix, `class`, metricName), tags, unit)
  }

  def timer(prefix: MetricPrefix, `class`: Class[_], metricName: String,
    tags: Map[String, String] = Map.empty, unit: Time = Time.Nanoseconds): Timer = {
    HistogramTimer(name(prefix, `class`, metricName), tags, unit)
  }

  def timer(metricName: String, tags: Map[String, String], unit: Time): Timer = {
    HistogramTimer(name(metricName), tags, unit)
  }

  def timer(metricName: String, unit: Time): Timer = {
    timer(metricName, Map.empty[String, String], unit)
  }

  def timer(metricName: String, tags: Map[String, String]): Timer = {
    timer(metricName, tags, Time.Nanoseconds)
  }

  def timer(metricName: String): Timer = {
    timer(metricName, Map.empty[String, String], Time.Nanoseconds)
  }

  def subscribe(actorRef: ActorRef, filter: SubscriptionFilter = AcceptAllFilter): Done = {
    Kamon.metrics.subscribe(filter, actorRef)
    Done
  }

  private[this] var metrics: TickMetricSnapshot = {
    val now = MilliTimestamp.now
    TickMetricSnapshot(now, now, Map.empty)
  }

  // returns the current snapshot. Doesn't collect until `start` is called
  def snapshot(): TickMetricSnapshot = metrics

  // Starts collecting snapshots.
  def start(actorRefFactory: ActorRefFactory, config: MetricsReporterConf): Done = {
    class SubscriberActor() extends Actor {
      val slidingAverageSnapshot: SlidingAverageSnapshot = new SlidingAverageSnapshot(
        Duration.ofSeconds(config.averagingWindowSizeSeconds.getOrElse(30L))
      )

      override def receive: Actor.Receive = {
        case snapshot: TickMetricSnapshot =>
          metrics = slidingAverageSnapshot.updateWithTick(snapshot)
      }
    }
    subscribe(actorRefFactory.actorOf(Props(new SubscriberActor)))
  }
}
