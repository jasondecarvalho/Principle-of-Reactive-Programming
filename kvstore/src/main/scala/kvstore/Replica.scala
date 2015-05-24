package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}

import scala.concurrent.duration.{Duration, _}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import context.dispatcher

  var kv = Map.empty[String, String]
  var persistence = context.actorOf(persistenceProps)
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistAcks = Map.empty[Long, (ActorRef, Cancellable)]

  var nextSeq = 0

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(key, value, id) => {
      kv += key -> value
      sender ! OperationAck(id)
    }

    case Remove(key, id) => {
      kv -= key
      sender ! OperationAck(id)
    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
  }

  val replica: Receive = {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case Snapshot(key, valueOption, seq) => {
      if(seq < nextSeq) {
        sender ! SnapshotAck(key, seq)
      } else if(seq == nextSeq) {
        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }
        val persist: Persist = Persist(key, valueOption, seq)
        val cancellable: Cancellable = context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, persistence, persist)
        persistAcks += seq -> (sender, cancellable)
      }
    }

    case Persisted(key, id) => {
      val (actorRef, cancellable) = persistAcks(id)
      actorRef ! SnapshotAck(key, id)
      cancellable.cancel()
      nextSeq += 1
    }
  }

}