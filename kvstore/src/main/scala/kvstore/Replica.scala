package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._
import scala.language.postfixOps

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

  sealed case class RetryPersist()
  sealed case class FlushFailedCheck()
  sealed case class ReplicaFailedCheck()

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  println(s"Init Replica " + self)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  private var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  private var secondaryNodes = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  private def replicators = secondaryNodes.valuesIterator.toSet
  type PersistID = Long
  type OperationID = Long

  private sealed case class PersistAck(sender: ActorRef, persist: Persist, persistTimeout: Deadline)
  private var toPersist = Map.empty[PersistID, PersistAck]
  //private var primaryInsertRemoveTimeouts = Map.empty[Long, (ActorRef, Deadline)]

  private sealed case class ReplicationAck(sender: ActorRef, replicators: Set[ActorRef], update: Replicate, replicaTimeout: Deadline)
  private var toReplicate = Map.empty[OperationID, ReplicationAck]

  val persistNode = context.actorOf(persistenceProps)

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10) {
    case _: PersistenceException => Restart
    case _: Exception            => Escalate
  }

  // Retry any 
  val tick = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, RetryPersist())
  val tickPrimaryPresist = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, FlushFailedCheck())
  val tickPrimaryReplication = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, ReplicaFailedCheck())

  override def postStop() = {
    tick.cancel()
    tickPrimaryPresist.cancel()
    tickPrimaryReplication.cancel()
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  private def attemptReplicate(key: String, valueOption: Option[String], seq: Long, replicators : Set[ActorRef] = this.replicators) = {
    val queuedReplicate = Replicate(key, valueOption, seq)
    println(s"Attempting to replicate id: $seq for $sender to $replicators")
    replicators foreach { _ ! queuedReplicate }
    if (!replicators.isEmpty)
      toReplicate += (seq -> ReplicationAck(sender, replicators, queuedReplicate, 1.seconds.fromNow))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv + ((key, value))
      attemptPersist(key, Some(value), id)
      attemptReplicate(key, Some(value), id)
    }
    case Remove(key, id) => {
      kv = kv - ((key))
      attemptPersist(key, None, id)
      attemptReplicate(key, None, id)
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Replicated(key, id) => {
      println(s"Received Replicated for $id from $sender")
      val remaining = toReplicate(id).replicators - sender
      val newReplicate = toReplicate(id)
      val ackTo = toReplicate(id).sender
      toReplicate = toReplicate - id
      if (remaining.isEmpty) {
        // Don't ack unless we have persisted as well
        if (!toPersist.contains(id)) {
          println(s"Sending OperationAck for $id")
          ackTo ! OperationAck(id)
        }
      } else {
        val updated = newReplicate.copy(replicators = remaining)
        toReplicate = toReplicate + (id -> updated)
      }
    }
    case Replicas(replicas) => {
      val secondaryReplicas = replicas - self
      println(s"Known replicas: $secondaryNodes")

      val joinedReplicas = secondaryReplicas -- secondaryNodes.keySet
      val joinedNodes = for { newReplica <- joinedReplicas } yield (newReplica, context.actorOf(Replicator.props(newReplica)))
      println(s"Synching new replicas for $joinedNodes")
      var initSeq = 0;
      kv foreach {
        case (k, v) =>
          attemptReplicate(k, Some(v), initSeq, joinedNodes.unzip._2);
          initSeq = initSeq + 1
      }

      val droppedReplicas = secondaryNodes.keySet.diff(secondaryReplicas)
      println(s"Dropped replicas: $droppedReplicas")
      droppedReplicas foreach { secondaryNodes(_) ! PoisonPill }
      
      println(s"Waiving outstanding acknowledgements for dropped replicas")
      val nodes2Drop = secondaryNodes.filter { case (k, v) => droppedReplicas.contains(k) }
      val replicators2Drop = nodes2Drop.map(_.swap)
      val toReplicateWithoutDroppedReplicas = toReplicate.map({
        case (id, repack) => {
          val remaining = repack.replicators -- replicators2Drop.keys
          val updated = repack.copy(replicators = remaining)
          id -> updated
        }
      })
      val acksToWaive = toReplicateWithoutDroppedReplicas.filter({case(id,repack) => repack.replicators.isEmpty})
      toReplicateWithoutDroppedReplicas foreach { case(id, repack) =>
        if (!toPersist.contains(id)) {
          println(s"Sending OperationAck for $id")
          repack.sender ! OperationAck(id)
        }
      }
      toReplicate = toReplicate -- acksToWaive.keys 

      secondaryNodes = secondaryNodes ++ joinedNodes -- droppedReplicas
    }
    case Persisted(key, id) if toPersist.contains(id) => {
      // Don't ack unless all as well
      if (!toReplicate.contains(id)) {
        println(s"Received primary persist ack for $id")
        toPersist(id).sender ! OperationAck(id)
      }
      toPersist = toPersist - id
    }
    case Persisted(key, id) => {
      println("Warning - Duplicate/unknown primary persist attempt for $id")
    }
    case RetryPersist() => {
      //println("Problem persisting - Retrying all queued Persists")
      toPersist foreach { case (_, perack) => persistNode ! perack.persist }
    }
    case FlushFailedCheck() => {
      val timedOut = toPersist.filter({ case (_, perack) => perack.persistTimeout.isOverdue() })
      if (!timedOut.isEmpty) println(s"Failing persist $timedOut")
      timedOut foreach { case (id, perack) => perack.sender ! OperationFailed(id) }
      // After notify we can drop flush timeout
      toPersist = toPersist -- timedOut.keys
    }
    case ReplicaFailedCheck() => {
      //println("Replicate failed!")
      val timedOut = toReplicate.filter({ case (id, repack) => repack.replicaTimeout.isOverdue() })
      if (!timedOut.isEmpty) println(s"Failing replication $timedOut")
      timedOut foreach { case (id, repack) => repack.sender ! OperationFailed(id) }
      toReplicate = toReplicate -- timedOut.keys
    }
    case _ => println("Unknown message")
  }

  var expectedSeq = 0L

  private def attemptPersist(key: String, valueOption: Option[String], seq: Long) = {
    val queuedPersist = Persist(key, valueOption, seq)
    println(s"Attempting to persist id: $seq")
    persistNode ! queuedPersist
    val persistID: PersistID = seq
    toPersist += (persistID -> PersistAck(sender, queuedPersist, 1.seconds.fromNow))
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key, valueOption, seq) if seq < expectedSeq => {
      println(s"Sequence number $seq is less than expected sequence $expectedSeq")
      attemptPersist(key, valueOption, seq)
    }
    case Snapshot(key, valueOption, seq) if seq > expectedSeq => {
      println(s"Sequence number $seq is greater than expected sequence $expectedSeq")
    }
    case Snapshot(key, valueOption, seq) => {
      println(s"Replica received snapshot request from $sender for $seq")
      valueOption match {
        case Some(newValue) => {
          println(s"Updating replica with ($key , $newValue)")
          kv = kv + ((key, newValue))
        }
        case None => kv = kv - key
      }
      expectedSeq += 1

      attemptPersist(key, valueOption, seq)
    }
    case Persisted(key, id) if toPersist.contains(id) => {
      println(s"Received persist ack for $id")
      toPersist(id).sender ! SnapshotAck(key, id)
      toPersist = toPersist - id
    }
    case Persisted(key, id) => {
      println("Warning - Duplicate/unknown persist attempt for $id")
    }
    case RetryPersist() => {
      //println("Problem persisting - Retrying all queued Persists")
      toPersist foreach { case (_, perack) => persistNode ! perack.persist }
    }

    case _ => println("Unknown message")
  }

  arbiter ! Join

}

