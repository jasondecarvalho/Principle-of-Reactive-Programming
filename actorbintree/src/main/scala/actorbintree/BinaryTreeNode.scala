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

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Insert(requester, id, newElem) => {
      val response: OperationFinished = OperationFinished(id)
      def tellInsertResponse = {
        context.parent ! response
        requester ! response
      }

      if (newElem == elem) {
        removed = false
        tellInsertResponse
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Insert(requester, id, newElem)
        } else {
          subtrees += Left -> context.actorOf(BinaryTreeNode.props(newElem, false))
          tellInsertResponse
        }
      } else if (elem < newElem) {
        if (subtrees contains Right) {
          subtrees(Right) ! Insert(requester, id, newElem)
        } else {
          subtrees += Right -> context.actorOf(BinaryTreeNode.props(newElem, false))
          tellInsertResponse
        }
      }
    }

    case Contains(requester, id, value) => {

      def tellContainsResponse(result: Boolean) = {
        val response = ContainsResult(id, result)
        requester ! response
        context.parent ! response
      }

      if (value == elem && !removed) {
        tellContainsResponse(true)
      } else if (value < elem && subtrees.contains(Left)) {
        subtrees(Left) ! Contains(requester, id, value)
      } else if (value > elem && subtrees.contains(Right)) {
        subtrees(Right) ! Contains(requester, id, value)
      } else {
        tellContainsResponse(false)
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
        context.parent ! response
        requester ! response
      }
    }

    case operationReply: OperationReply =>
      context.parent ! operationReply
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
