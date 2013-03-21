package cache

import util.ActorRefStop
import akka.actor._
import akka.actor.Scheduler
import akka.pattern.ask
import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit.{ MILLISECONDS, NANOSECONDS }
import scala.collection.mutable.{ Map, HashMap }
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * A stage is a particular kind of cache that is used for staging IO updates.
 * Many kinds of IO updates can be combined (e.g. instead of writing a single
 * log line to a file, you can collect ten lines and write them all at once).
 * This has the capacity to greatly improve performance when IO is a limiting
 * factor.
 * <p>
 * Built on a cache, stage supports standard eviction based on a time to live .
 * <p>
 * Stopping a stage evicts all entries from the stage. As part of shutdown, in
 * order to avoid data loss, every stage should be stopped.
 */
trait Stage[K, V] extends Map[K, V] { // with FutureDeliveryStrategySequential

  def expirationPolicy: ExpirationPolicy
  def coalesce: (K, V, V) => V
  def evict: (K, V) => Unit
  def scheduler: ExecutorService

  private sealed trait Request
  private sealed trait Response

  private case class  Add(k: K, v: V) extends Request
  private case class  Get(k: K) extends Request
  private case object GetAll extends Request
  private case class  Evict(k: K) extends Request
  private case class  Flush(timeout: Long) extends Request
  private case class  Remove(k: K) extends Request
  private case object Stop extends Request

  private case class  Got(v: Option[V]) extends Response
  private case class  GotAll(list: List[(K, V)]) extends Response
  private case class  Removed(v: Option[V]) extends Response
  private case object Stopped extends Response

  private case class FlushJob(createTime: Long, key: K)

  private val actorSystem = ActorSystem("livingsocialstage")

  private val actor: ActorRef = actorSystem.actorOf(Props(new StageActor))

  private implicit val timeout: Timeout = Timeout(Duration(10000, TimeUnit.MILLISECONDS))

  def get(key: K): Option[V] = Await.result(actor ? Get(key), timeout.duration) match {
    case Got(v) => v
    case _ => None
  }

  /**
   * Asynchronously retrieves the value for the specified key.
   */
  // def getLater(key: K): Future[Option[V]] = (actor !! (Get(key), { case Got(v) => v }))

  def iterator: Iterator[(K, V)] = Await.result(actor ? GetAll, timeout.duration) match {
    case GotAll(all) => all.iterator
    case _ => sys.error("Unable to GetAll")
  }

  def += (kv: (K, V)) = {
    actor ! Add(kv._1, kv._2)
    this
  }

  def -= (key: K) = {
    actor ! Remove(key)
    this
  }

  /**
   * Starts the stage. This function is called automatically when the stage
   * is created.
   */
  def start = {
    val timeout = expirationPolicy.timeToLive(MILLISECONDS).getOrElse[Long](1000)
    actorSystem.scheduler.schedule(Duration(timeout, MILLISECONDS), Duration(timeout, MILLISECONDS), actor, Flush(timeout))
  }

  /**
   * Stops the stage and evicts all entries.
   */
  def stop = for {
    _ <- actor ? Stop
    _ <- ActorRefStop(actorSystem, timeout).stop(actor)
    _ <- Future.successful(actorSystem.shutdown)
  } yield ()


  private def scheduleTimeToLive(expirable: Expirable[K, V]) {
    expirable.policy.timeToLive(MILLISECONDS) match {
      case None =>
      case Some(timeToLive) => {
        val ref = new WeakReference(expirable)
        val runnable = new Runnable {
          def run = {
            handleExpiration(ref)
          }
        }
        scheduler.submit(runnable)
      }
    }
  }

  private def handleExpiration(expirableRef: WeakReference[Expirable[K, V]]) {
    Option(expirableRef.get).map { expirable =>
      expirationPolicy.timeToLiveNanos match {
        case Some(timeout) => {
          if(isExpired(expirable, timeout)) {
            actor ! Evict(expirable.key)
          }
        }
        case _ =>
      }
    }
  }

  private def isExpired(expirable: Expirable[K, V], timeout: Long) = (System.nanoTime - expirable.creationTimeNanos) >= timeout

  private class StageActor extends Actor{
    val delegate = new HashMap[K,  Expirable[K, V]]()

    def receive = {
      case Get(k) => sender ! Got(delegate.get(k).map(_.value))

      case Add(k, v2) => {
        val expirable = delegate.get(k) match {
          case None     => Expirable(k, v2, expirationPolicy)
          case Some(v1) => Expirable(k, coalesce(k, v1.value, v2), expirationPolicy, v1.creationTimeNanos, NANOSECONDS)
        }

        delegate.update(k, expirable)
        scheduleTimeToLive(expirable)
      }

      case Remove(k) => {
        val v = delegate.get(k).map(_.value)
        delegate -= k
      }

      case Evict(k) => delegate.get(k).map(_.value).map { v =>
        delegate -= k
        evict(k, v)
      }

      case Flush(timeout: Long) => delegate.values.foreach { v =>
        if(isExpired(v, timeout)) self ! Evict(v.key)
      }

      case Stop => {
        delegate.foreach { entry =>
          evict(entry._1, entry._2.value)
        }

        delegate.clear

        sender ! Stopped
      }

      case GetAll => sender ! GotAll(delegate.toList.map(t => (t._1, t._2.value)))

      case _ => sys.error("received unknown message")
    }
  }
}


object Stage {
  def apply[K, V](expirationPolicy_ : ExpirationPolicy, coalesce_ : (K, V, V) => V, evict_ : (K, V) => Unit, poolSize: Int = 20) = new Stage[K, V] {
    val expirationPolicy = expirationPolicy_

    val coalesce = coalesce_

    val evict = evict_

    val scheduler = Executors.newFixedThreadPool(poolSize)

    start
  }
}