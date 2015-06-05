package quickcheck

import common._
import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("delmin1") = forAll { a: Int =>
    val h = insert(a, empty)
    val d = deleteMin(h)
    isEmpty(d)
  }

  property("twomin1") = forAll { (a: Int, b: Int) =>
    val minVal = math.min(a, b)
    
    val h = insert(a, empty)
    val h2 = insert(b,h)
    findMin(h2) == minVal
  }

  property("insertmin1") = forAll { (a: Int, h: H) =>
    val mina = findMin(h)
    val h2 = insert(a,h)
    findMin(h2) == math.min(mina,a)
  }

  property("delmins") = forAll { (h: H) =>
    if (!isEmpty(h)){      
      val mina = findMin(h) 
      val h2 = deleteMin(h)
      
      if (!isEmpty(h2)) {
        findMin(h2) >= mina
      }
    }
    true
  }

  property("insdelmins") = forAll { (h: H) =>
    if (!isEmpty(h)){      
      val mina = findMin(h)
      val h2 = insert(math.min(mina, mina-1), h)
      val h3 = deleteMin(h2)
      
      if (!isEmpty(h3)) {
        findMin(h3) >= mina
      }
    }
    true
  }
  
  
  property("meldmin") = forAll { (h1 :H, h2 : H) =>
    val m1 = if (isEmpty(h1)) 0 else findMin(h1)
    val m2 = if (isEmpty(h2)) 0 else findMin(h2)
    val mh = meld(h1, h2)
    
    findMin(mh)== math.min(m1,m2)
  }

  property("meldmin") = forAll { (sh1 :Seq[Int]) =>
    if (sh1.isEmpty)
      true
    else {
      def meldSeqToH(s:Seq[Int]) : H = 
        {
          s match { 
          case Nil => empty
          case head +: Nil => insert(head, empty)
          case head +: tail => meld(insert(head, empty), meldSeqToH(tail)) 
        }
        }
      val minSh1 = sh1.min

      val withoutEmpties = meldSeqToH(sh1)
      findMin(withoutEmpties) == minSh1
    }
  }
  
  
  property("nomeldempty") = forAll { (h1 :H, h2 : H) =>
    val mh = meld(h1, h2)
    
    empty != mh && (!isEmpty(h1) && !isEmpty(h2))
  }
  
  
  property("sortedSeq") = forAll { (intSeq : Seq[Int]) =>
    def hToList(h : H, s : Seq[Int]) : (Seq[Int], H) = {
        if (isEmpty(h))
          (s,h)
        else {
          hToList(deleteMin(h), s ++ Seq(findMin(h)))
        }
    }

    def seqToH(s:Seq[Int]) : H = 
      {
        s match { 
        case Nil => empty
        case head +: Nil => insert(head, empty)
        case head +: tail => insert(head, seqToH(tail)) 
      }
      }
      
    
    val sortSeq = intSeq.sorted
    val h1 = seqToH(sortSeq)
    
    val retList = hToList(h1,Seq.empty)
    val sortedRetList = sortSeq 
    sortedRetList == retList._1
  }

  
  
  lazy val genHeap: Gen[H] = for {
    v <-arbitrary[Int]
    h <- frequency((1,const(empty)),(19,genHeap))    
  } yield insert(v,h)


  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }  
  
  
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
