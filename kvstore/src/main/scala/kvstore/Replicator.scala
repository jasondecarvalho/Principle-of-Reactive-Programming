package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  // map from sequence number -> (sender, request, cancellable)
  var snapshotAcks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case replicate@Replicate(key, valueOption, id) => {
      val seq = nextSeq
      val snapshot = Snapshot(key, valueOption, seq)
      val cancellable: Cancellable = context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, replica, snapshot)
      snapshotAcks += seq -> (sender, replicate, cancellable)
    }

    case SnapshotAck(key, seq) => {
      val (actorRef, replicate, cancellable) = snapshotAcks(seq)
      actorRef ! Replicated(key, replicate.id)
      snapshotAcks -= seq
      cancellable.cancel()
    }
  }

}
