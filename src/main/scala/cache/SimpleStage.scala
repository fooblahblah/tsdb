package cache

import scala.concurrent.stm._
import scalaz.Semigroup
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

case class Expiring[V](created: Long, value: V)

trait SimpleStage[K, V] {
  private val cache = TMap.empty[K, Expiring[V]]

  def atCapacity: (V) => Boolean
  def evict: (K, V) => Unit
  def semigroup: Semigroup[V]
  def duration: Duration

  def put(k: K, v: V) = atomic { implicit tx =>
    val ts = System.currentTimeMillis()

    val update = Expiring(ts, cache.get(k).map { exp =>
      semigroup.append(exp.value, v)
    }.getOrElse(v))

    cache.put(k, update)

    if(atCapacity(update.value)) flush(k)
    threadPool.schedule(flushHandler(k), duration.toUnit(TimeUnit.MILLISECONDS).toLong, TimeUnit.MILLISECONDS)
  }

  private def flush(k: K) = atomic { implicit tx =>
    cache.remove(k).foreach { exp =>
      evict(k, exp.value)
    }
  }

  private def flushHandler(k: K) = new Runnable {
    def run {
      cache.snapshot.get(k).map { exp =>
        if(System.currentTimeMillis() - exp.created >= duration.toUnit(TimeUnit.MILLISECONDS).toLong) {
          flush(k)
        }
      }
    }
  }

  def stop: Future[Unit] = atomic { implicit tx =>
    Future {
      if(cache.isEmpty) {
        println("shutting down threadpool")
        threadPool.shutdown()
      } else {
        Thread.sleep(500)
        stop map (f => f)
      }
    }
  }

  private val threadPool = Executors.newScheduledThreadPool(8)
}

object SimpleStage {
  def apply[K,V](_duration: Duration, _atCapacity: (V) => Boolean, _evict: (K, V) => Unit)(implicit _semigroup: Semigroup[V]) = {
    new SimpleStage[K, V] {
      val atCapacity = _atCapacity
      val duration   = _duration
      val evict      = _evict
      val semigroup  = _semigroup
    }
  }
}