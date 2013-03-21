package cache

import scala.concurrent.stm._
import scalaz.Semigroup
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

trait SimpleStage[K, V] {
  private val cache = TMap[K, V]()

  def atCapacity: (V) => Boolean
  def evict: (K, V) => Unit
  def semigroup: Semigroup[V]
  def duration: Duration

  def put(k: K, v: V) = atomic { implicit tx =>
    cache.put(k, cache.get(k).map { cur =>
      semigroup.append(cur, v)
    }.getOrElse(v)).map { update =>
      if(atCapacity(update)) flush(k)//threadPool.schedule(flushHandler(k), duration.toUnit(TimeUnit.MILLISECONDS).toLong, TimeUnit.MILLISECONDS)
    }
  }

  private def flush(k: K) = atomic { implicit tx =>
    cache.remove(k).foreach { v =>
      evict(k, v)
    }
  }

  private def flushHandler(k: K) = new Runnable {
    def run {
      flush(k)
    }
  }

  def stop = atomic { implicit tx =>
//    Future(if(!m.isEmpty))
//    threadPool.shutdown()
    Future.successful(())
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