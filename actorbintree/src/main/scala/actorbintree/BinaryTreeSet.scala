/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester, id, elem) => {
      println(s"Insert $elem with id $id")
      root ! Insert(requester, id, elem)
    }
    case Remove(requester, id, elem) => {
      println(s"Remove $elem with id $id")
      root ! Remove(requester, id, elem)
    }
    case Contains(requester, id, elem) => {
      println(s"Contains $elem with id $id")
      root ! Contains(requester, id, elem)
    }
    case GC => {
      val oldRoot = root
      val newRoot = createRoot
      println("Switching to GC mode")
      root = newRoot
      context.become(garbageCollecting(newRoot))
      oldRoot ! CopyTo(newRoot)
    }
    case _ => println("Unknown message")
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case Insert(r, i, e)   => pendingQueue = pendingQueue :+ Insert(r, i, e)
    case Remove(r, i, e)   => pendingQueue = pendingQueue :+ Remove(r, i, e)
    case Contains(r, i, e) => pendingQueue = pendingQueue :+ Contains(r, i, e)
    case GC                => println("Duplicate GC message detected. Ignoring.")
    case CopyFinished => {
      println("Switching back to normal mode")
      context.become(normal)
      println("Killing old root")
      sender() ! PoisonPill
      pendingQueue foreach { x => self ! x }
      pendingQueue = Queue.empty[Operation]
    }
    case _ => println("Unknown message during GC")
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def insNode(requester: ActorRef, id: Int, elem2Insert: Int) = {
    if (elem2Insert == elem) {
      removed = false
      requester ! OperationFinished(id)
    } else {
      val side = if (elem2Insert < elem) Left else Right
      if (subtrees.isDefinedAt(side)) {
        subtrees(side) ! Insert(requester, id, elem2Insert)
      } else {
        subtrees += (side -> context.actorOf(BinaryTreeNode.props(elem2Insert, false)))
        requester ! OperationFinished(id)
      }
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem2Insert) => insNode(requester, id, elem2Insert)
    case Remove(requester, id, elem2Remove) => {
      if (elem2Remove == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val side = if (elem2Remove < elem) Left else Right
        if (subtrees.isDefinedAt(side)) {
          subtrees(side) ! Remove(requester, id, elem2Remove)
        } else {
          println(s"Attempt to remove $elem that isn't found in the tree")
          requester ! OperationFinished(id)
        }
      }
    }
    case Contains(requester, id, elem2Find) => {
      if (elem2Find == elem)
        requester ! ContainsResult(id, !removed)
      else {
        val side = if (elem2Find < elem) Left else Right
        if (subtrees.isDefinedAt(side)) {
          subtrees(side) ! Contains(requester, id, elem2Find)
        } else {
          println(s"$elem2Find not found for request $id")
          requester ! ContainsResult(id, false)
        }

      }
    }
    case CopyTo(target) => {
      // Nothing to do in null case
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
      } else {
        // Removed nodes are by definition confirmed to be copied
        context.become(copying(subtrees.values.toSet, removed))
        if (!removed) {
          target ! Insert(self, Integer.MIN_VALUE, elem)
        }
        subtrees foreach { case (k, v) => v ! CopyTo(target) }
      }
    }

    case _ => println("Unknown message")
  }

  // optional
  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case Insert(requester, id, elem2Insert) => insNode(requester, id, elem2Insert)
    case OperationFinished(id) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
      } else
        context.become(copying(expected, true))
    }
    
    case CopyFinished => {
      val remainingExpected = expected - sender()
      if (insertConfirmed) {
        context.parent ! CopyFinished
      } else
        context.become(copying(remainingExpected, insertConfirmed))
    }
    case _ => println("Unknown message while copying")
  }

}
