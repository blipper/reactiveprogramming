package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Scheduler
import scala.concurrent.duration._
import akka.actor.ActorSystem
import scala.language.postfixOps
import akka.actor.TypedActor.PostStop

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  case class Sync(key: Map[String,String])

  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class RetrySnapshot()

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  println(s"Init Replicator $self for replica $replica" )
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  private sealed case class ReplicateAck(replica : ActorRef, sender : ActorRef, toRep : Replicate)
  // map from sequence number to pair of sender and request
  private var acks = Map.empty[Long, ReplicateAck]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
   override def postStop() = tick.cancel()

  // Retry any 
  val tick = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, RetrySnapshot())
  
   
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => {
      println(s"Replicator received Replicate for $id and requesting snapshot from replica $replica")
      val ns = nextSeq
      val toAck = (ns, ReplicateAck(replica,sender,Replicate(key, valueOption, id)))
      acks = acks + toAck
      replica ! Snapshot(key,valueOption,ns)
    }
    case SnapshotAck(key, seq) => {
      println(s"Ack'ing $seq")
      acks(seq).sender ! Replicated(acks(seq).toRep.key, acks(seq).toRep.id) 
      acks = acks - seq
    }
    case RetrySnapshot() => {
      acks foreach {case (seq, repAck) => replica ! Snapshot(repAck.toRep.key,repAck.toRep.valueOption,seq)} 
    }
    case _ => println("Unknown message received")
  }

}
