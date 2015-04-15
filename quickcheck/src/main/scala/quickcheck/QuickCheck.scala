package quickcheck


import java.util.Collections

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: A =>
    val h = insert(a, empty)
    findMin(h) equals a
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) equals m
  }

  property("Minimum of a size 2 heap is the smallest element") = forAll { (a: A, b: A) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == List(a, b).min
  }

  property("Minimum of a size 3 heap is the smallest element") = forAll { (a: Int, b: Int, c: Int) =>
    val h = insert(c, insert(b, insert(a, empty)))
    findMin(h) == List(a, b, c).min
  }

  property("Maximum of a size 3 heap is the largest element") = forAll { (a: Int, b: Int, c: Int) =>
    val h = insert(c, insert(b, insert(a, empty)))
    findMin(deleteMin(deleteMin(h))) == List(a, b, c).max
  }

  property("Inserting an element into an empty heap, then deleting the min results in an empty heap") = forAll { a: A =>
    val h1 = deleteMin(insert(a, empty))
    isEmpty(h1)
  }

  property("Inserting 2, then deleting 2, results in empty map") = forAll { (a: A, b: A) =>
    val h1 = deleteMin(deleteMin(insert(b, insert(a, empty))))
    isEmpty(h1)
  }

  property("The minimum of two heaps melded, is the minimum of the minima of the heaps") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == List(findMin(h1), findMin(h2)).min
  }

  property("The minimum of a heap melded with the empty heap is the same as the minimum of the first heap") = forAll { (h: H) =>
    findMin(meld(h, empty)) == findMin(h)
  }

  property("The minimum of a heap melded with itself, is the same as the minimum of itself") = forAll { (h: H) =>
    findMin(meld(h, h)) == findMin(h)
  }

  lazy val genHeap: Gen[H] = for {
    arbitrary <- arbitrary[A]
    heap <- oneOf(const(empty), genHeap)
  } yield insert(arbitrary, heap)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
