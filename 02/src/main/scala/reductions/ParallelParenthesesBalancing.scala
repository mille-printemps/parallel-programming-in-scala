package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def rbalance(chars: Array[Char], index: Int, count: Int, balanced: Boolean): Boolean =
      if (index == chars.length) {
        if(0 == count && balanced == true) {
          true
        } else {
          false
        }
      } else if (chars(index) == '(') {
        if (count < 0) {
          rbalance(chars, index+1, count+1, false)
        } else {
          rbalance(chars, index+1, count+1, balanced)
        }
      } else if (chars(index) == ')') {
        if (count < 0) {
          rbalance(chars, index+1, count-1, false)
        } else {
          rbalance(chars, index+1, count-1, balanced)
        }
      } else {
        rbalance(chars, index+1, count, balanced)
      }
    
    rbalance(chars, 0, 0, true)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  // The threshold must be greater then 2
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    // count non-balanced left and right parentheses
    def traverse(idx: Int, until: Int, left: Int, right: Int) : (Int, Int) = {
      var i: Int = idx
      var l: Int = left
      var r: Int = right
      
      while (i < until) {
        if (chars(i) == '(') {
          l += 1
        } else if (chars(i) == ')') {
          if (0 < l) {
            l -= 1
          } else {
            r += 1
          }
        } else {
          // nothing is done
        }
        i += 1
      }
      
      (l, r)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      
      def getNonBalanced(left: (Int, Int), right: (Int, Int)): (Int, Int) = {
        var numberOfLeft: Int = right._1
        var numberOfRight: Int = left._2
        
        val numberOfNonBalanced: Int = left._1 - right._2
        if (0 < numberOfNonBalanced) {
          numberOfLeft += numberOfNonBalanced
        } else if (numberOfNonBalanced < 0) {
          numberOfRight += math.abs(numberOfNonBalanced)            
        } else {
          // nothing is done
        }
        (numberOfLeft, numberOfRight)
      }
      
      def rreduce(from: Int, until: Int): (Int, Int) = {
        if (until - from <= threshold) {
          val(l, r) = traverse(from, until, 0, 0)
          (l, r)
        } else {
          val mid: Int = from + (until - from)/2
          val(l, r) = parallel(rreduce(from, mid), rreduce(mid, until))
          
          getNonBalanced(l, r)
        }
      }
      
      val(l, r) = rreduce(from, until)
      (l, r)
    }

    reduce(0, chars.length) == (0, 0)
  }
}
