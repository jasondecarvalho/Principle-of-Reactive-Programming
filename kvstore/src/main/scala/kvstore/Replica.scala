package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}

import scala.concurrent.duration._

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

  class PersistAck(val requester: ActorRef, val retry: Cancellable)

  class TimeoutPersistAck(requester: ActorRef, retry: Cancellable, val timeout: Cancellable)
    extends PersistAck(requester: ActorRef, retry: Cancellable)

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

  var snapshotAcks = Map.empty[Long, ActorRef]
  var persistAcks = Map.empty[Long, Cancellable]
  var replicateAcks = Map.empty[Long, Set[Cancellable]]
  var timeouts = Map.empty[Long, (Long, ActorRef, Cancellable)]

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

      added foreach {
        replica => {
          val replicator: ActorRef = context.actorOf(Replicator.props(replica))
          secondaries += replica -> replicator
          replicators += replicator
        }
      }

      removed foreach {
        replica => {
          secondaries -= replica
          replicators -= replica
        }
      }
    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case Insert(key, value, id) => {
      scheduleTimeout(id, 1 + secondaries.size)
      kv += key -> value
      persist(key, Some(value), id)
      replicate(key, Some(value), id)
    }

    case Remove(key, id) => {
      scheduleTimeout(id, 1 + secondaries.size)
      kv -= key
      persist(key, None, id)
      replicate(key, None, id)
    }

    case Persisted(_, id) => {
      persistAcks(id).cancel()
      persistAcks -= id
      registerCompletedOperation(id)
    }

    case Replicated(_, id) => {
      registerCompletedOperation(id)
    }
  }

  private def persist(key: String, valueOption: Option[String], id: Long) = {
    val persist: Persist = Persist(key, valueOption, id)
    val cancellable: Cancellable = context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, persistence, persist)
    persistAcks += id -> cancellable
  }

  private def replicate(key: String, valueOption: Option[String], id: Long) = {
    val replicate: Replicate = Replicate(key, valueOption, id)
    val cancellables: Set[Cancellable] = replicators.map {
      replicator => context.system.scheduler.schedule(Duration.Zero, 50 milliseconds, replicator, replicate)
    }
    replicateAcks += id -> cancellables
  }

  private def scheduleTimeout(id: Long, waitingOperations: Long) = {
    val operationFailed: OperationFailed = OperationFailed(id)
    val cancellable: Cancellable = context.system.scheduler.schedule(1 second, 1 day, sender, operationFailed)
    timeouts += id ->(waitingOperations, sender, cancellable)
  }

  private def registerCompletedOperation(id: Long) = {
    val (count, requester, timeout) = timeouts(id)
    if (count == 1) {
      timeout.cancel()
      timeouts -= id
      replicateAcks(id) foreach {
        _.cancel()
      }
      replicateAcks -= id
      requester ! OperationAck(id)
    } else {
      timeouts += id ->(count - 1, requester, timeout)
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
        snapshotAcks += seq -> sender
        persist(key, valueOption, seq)
      }
    }

    case Persisted(key, id) => {
      persistAcks(id).cancel()
      persistAcks -= id
      snapshotAcks(id) ! SnapshotAck(key, id)
      snapshotAcks -= id
      nextSeq += 1
    }
  }

}