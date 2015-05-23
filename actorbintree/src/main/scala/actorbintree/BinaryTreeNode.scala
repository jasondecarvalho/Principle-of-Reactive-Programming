package actorbintree

import actorbintree.BinaryTreeNode._
import akka.actor.{Stash, Actor, ActorRef, Props}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def receive = normal

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Insert(requester, id, newElem) => {
      if (newElem == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Insert(requester, id, newElem)
        } else {
          subtrees += Left -> context.actorOf(BinaryTreeNode.props(newElem, false))
          requester ! OperationFinished(id)
        }
      } else if (elem < newElem) {
        if (subtrees contains Right) {
          subtrees(Right) ! Insert(requester, id, newElem)
        } else {
          subtrees += Right -> context.actorOf(BinaryTreeNode.props(newElem, false))
          requester ! OperationFinished(id)
        }
      }
    }

    case Contains(requester, id, value) => {
      if (value == elem && !removed) {
        requester ! ContainsResult(id, true)
      } else if (value < elem && subtrees.contains(Left)) {
        subtrees(Left) ! Contains(requester, id, value)
      } else if (value > elem && subtrees.contains(Right)) {
        subtrees(Right) ! Contains(requester, id, value)
      } else {
        requester ! ContainsResult(id, false)
      }
    }

    case Remove(requester, id, value) => {
      if (value < elem && subtrees.contains(Left)) {
        subtrees(Left) ! Remove(requester, id, value)
      } else if (elem < value && subtrees.contains(Right)) {
        subtrees(Right) ! Remove(requester, id, value)
      } else {
        if (value == elem) {
          removed = true
        }
        val response: OperationFinished = OperationFinished(id)
        requester ! response
      }
    }

    case CopyTo(treeNode) => {
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
      } else {
        val subnodes: Set[ActorRef] = subtrees.values.toSet
        subnodes foreach {
          _ ! CopyTo(treeNode)
        }
        if (!removed) {
          treeNode ! Insert(self, 0, elem)
        }
        context.become(copying(subnodes, removed))
      }
    }

  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case operationFinished: OperationFinished => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.unbecome
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }
    }

    case CopyFinished => {
      val remaining = expected - sender
      if (remaining.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.unbecome
      } else {
        context.become(copying(remaining, insertConfirmed))
      }
    }
  }

}
