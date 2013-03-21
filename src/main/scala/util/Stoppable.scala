package util

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.ReceiveTimeout
import akka.util.Timeout
import scala.collection.immutable.Queue
import scalaz.Semigroup
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import akka.event.Logging
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.Await
import com.weiglewilczek.slf4s.Logging
import scala.util.Try

/**
 * A base trait for typeclass instances that describe how to stop a given type of service.
 * TODO: make polymorphic in the type of the Future, perhaps using an ADT such as StopResult { StopSuccess / StopFailure }
 *
 * In order for implementations of this trait to work properly in a graph where the same service is a dependant of multiple other services,
 * it should be safe for stop to be called multiple times with the same target.
 */
trait Stop[-A] {
  def stop(a: A): Future[Any]
}

/**
 * A DAG of things that can be stopped. Dependents are traversed in breadth-first order.
 */
sealed trait Stoppable { self =>
  protected def stop: Future[Any]
  def dependents: List[Stoppable]

  def append(other: Stoppable) = new Stoppable {
    protected def stop = self.stop zip other.stop
    def dependents = self.dependents ++ other.dependents
  }
}

object Stoppable extends Logging {
  def apply[A](a: A, deps: List[Stoppable] = Nil)(implicit stopa: Stop[A], ctx: ExecutionContext): Stoppable = new Stoppable {
    protected def stop = {
      Future(logger.info("About to stop " + a)) flatMap { _ =>
        stopa.stop(a).map {
          case v => logger.info("Stopped " + a + " with result " + v)
        }.recover { case ex =>
          logger.error("Stop failed", ex)
        }
      }
    }

    def dependents = deps
  }

  def fromFuture(future: => Future[Any]) = new Stoppable {
    protected lazy val stop = future
    val dependents = Nil
  }

  implicit object semigroup extends Semigroup[Stoppable] {
    def append(a: Stoppable, b: => Stoppable): Stoppable = a append b
  }

  /**
   * Stops the specified stoppable, returning a future containing a list of the results
   * of the stoppable graph in breadth-first order. When this future is completed,
   * everything will be stopped.
   *
   * TODO: Make it possible to specify whether failure to stop any given service
   * should prevent the stopping of its dependants. At present, any exception encountered
   * in stopping will stop the stopping process, leaving the system in a potentially
   * indeterminate state.
   */
  implicit def stoppableStop(duration: Duration)(implicit ctx: ExecutionContext): Stop[Stoppable] = new Stop[Stoppable] {
    def stop(stoppable: Stoppable) = {
      def _stop(q: Queue[List[Stoppable]]): Future[List[Any]] = {
        if (q.isEmpty) Future(Nil)
        else {
          val (xs, remainder) = q.dequeue
          Future.sequence(xs.map(s => Future(Await.result(s.stop, duration)))).flatMap(r => _stop(remainder ++ xs.map(_.dependents)).map(r ::: _))
                 //Future.sequence(xs.map(_.stop)).flatMap(r => _stop(remainder ++ xs.map(_.dependents)).map(r ::: _))
        }
      }

      _stop(Queue(List(stoppable)))
    }
  }

  implicit def stoppableStop(implicit ctx: ExecutionContext): Stop[Stoppable]  = stoppableStop(Duration.Inf)


  def stop(stoppable: Stoppable, duration: Duration = Duration.Inf)(implicit ctx: ExecutionContext) =
    stoppableStop(duration).stop(stoppable)
}

case class ActorRefStop(actorSystem: ActorSystem, timeout: Timeout) extends Stop[ActorRef] {
  class StopMonitor(target: ActorRef, result: Promise[Unit], timeout: Timeout) extends Actor {
    // Terminated will be received when target has been stopped
    context watch target
    // ReceiveTimeout will be received if nothing else is received within the timeout
    context.setReceiveTimeout(timeout.duration)

    def receive = {
      case Terminated(a) ⇒
        self ! PoisonPill

      case ReceiveTimeout ⇒
        result.complete(Try(new RuntimeException("Failed to stop [%s] within [%s]".format(target.path, context.receiveTimeout))))
        self ! PoisonPill
    }
  }

  override def stop(target: ActorRef): Future[Unit] = {
    val exitFuture = Promise[Unit].success(actorSystem.dispatcher)
    val stopMonitor = actorSystem.actorOf(Props(new StopMonitor(target, exitFuture, timeout)))
    target ! PoisonPill
    exitFuture.future
  }
}


// vim: set ts=4 sw=4 et:
