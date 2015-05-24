package kvstore

import akka.actor.{Actor, ActorRef, Props}
import kvstore.Arbiter._

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

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

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

    case Replicate(key, valueOption, id) => {
      valueOption match {
        case Some(value) => {
          kv += key -> value
          sender ! Replicated(key, id)
        }
        case None => {
          kv -= key
          sender ! Replicated(key, id)
        }
      }
    }

    case Snapshot(key, valueOption, seq) => {
      if(seq < nextSeq) {
        sender ! SnapshotAck(key, seq)
      } else if(seq == nextSeq) {
        valueOption match {
          case Some(value) => {
            kv += key -> value
            sender ! SnapshotAck(key, seq)
          }
          case None => {
            kv -= key
            sender ! SnapshotAck(key, seq)
          }
        }
        nextSeq += 1
      }
    }
  }

}