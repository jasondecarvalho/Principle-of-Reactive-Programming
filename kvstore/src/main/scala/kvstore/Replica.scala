package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}

import scala.concurrent.duration._
import scala.util.Random

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

  var snapshotRequesters = Map.empty[Long, ActorRef]

  var persistAcks = Map.empty[Long, Cancellable]
  var replicateAcks = Map.empty[Long, Set[(ActorRef, Cancellable)]] // Changing this data structure might make the code below better
  var pendingOps = Map.empty[Long, (ActorRef, Cancellable)]

  var nextSeq = 0

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Replicas(replicas) => {
      val added = replicas -- secondaries.keySet - self
      val removed = secondaries.keySet -- replicas

      removed foreach {
        replica => {
          var newReplicateAcks = Map.empty[Long, Set[(ActorRef, Cancellable)]]
          var idsToCheck = Set.empty[Long]
          replicateAcks foreach {
            case (id, remainingAcks) => {
              remainingAcks foreach {
                case (actorRef, cancellable) if actorRef == secondaries(replica) => {
                  cancellable.cancel()
                }
              }
              val withoutRemovedReplicas = remainingAcks filterNot {
                case (actorRef, _) => actorRef == secondaries(replica)
              }
              if(!withoutRemovedReplicas.isEmpty) {
                newReplicateAcks += id -> withoutRemovedReplicas
              } else {
                idsToCheck += id
              }
            }
          }
          replicateAcks = newReplicateAcks

          idsToCheck foreach { checkForCompletedOperation(_) }

          secondaries(replica) ! PoisonPill
          secondaries -= replica
          replicators -= replica
        }
      }

      added foreach {
        replica => {
          val replicator: ActorRef = context.actorOf(Replicator.props(replica))
          secondaries += replica -> replicator
          replicators += replicator
        }
      }

      kv foreach {
        case (key, value) => replicate(key, Some(value), Random.nextLong)
      }
    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case Insert(key, value, id) => {
      scheduleTimeout(id)
      kv += key -> value
      persist(key, Some(value), id)
      replicate(key, Some(value), id)
    }

    case Remove(key, id) => {
      scheduleTimeout(id)
      kv -= key
      persist(key, None, id)
      replicate(key, None, id)
    }

    case Persisted(_, id) => {
      persistAcks(id).cancel()
      persistAcks -= id
      checkForCompletedOperation(id)
    }

    case Replicated(_, id) => {
      val remaining = replicateAcks(id) filterNot {
        case (actorRef, _) => actorRef == sender
      }
      if(remaining.isEmpty) {
        replicateAcks -= id
      } else {
        replicateAcks += id -> remaining
      }
      checkForCompletedOperation(id)
    }
  }

  private def persist(key: String, valueOption: Option[String], id: Long) = {
    val persist: Persist = Persist(key, valueOption, id)
    val cancellable: Cancellable = context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, persistence, persist)
    persistAcks += id -> cancellable
  }

  private def replicate(key: String, valueOption: Option[String], id: Long) = {
    if(!replicators.isEmpty) {
      val replicate: Replicate = Replicate(key, valueOption, id)
      var replicateAcksSet: Set[(ActorRef, Cancellable)] = Set.empty[(ActorRef, Cancellable)]
      replicators foreach {
        replicator =>
          val cancellable = context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, replicator, replicate)
          replicateAcksSet += ((replicator, cancellable))
      }
      replicateAcks += id -> replicateAcksSet
    }
  }

  private def scheduleTimeout(id: Long) = {
    val operationFailed: OperationFailed = OperationFailed(id)
    val cancellable: Cancellable = context.system.scheduler.schedule(1 second, 1 day, sender, operationFailed)
    pendingOps += id ->(sender, cancellable)
  }

  private def checkForCompletedOperation(id: Long) = {
    if(pendingOps contains id) {
      val (requester, timeout) = pendingOps(id)
      if (!persistAcks.contains(id) && !replicateAcks.contains(id)) {
        timeout.cancel()
        pendingOps -= id
        requester ! OperationAck(id)
      }
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
        snapshotRequesters += seq -> sender
        persist(key, valueOption, seq)
      }
    }

    case Persisted(key, id) => {
      persistAcks(id).cancel()
      persistAcks -= id
      snapshotRequesters(id) ! SnapshotAck(key, id)
      snapshotRequesters -= id
      nextSeq += 1
    }
  }

}