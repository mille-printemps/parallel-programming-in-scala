package kmeans

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import scala.math._

object KM extends KMeans
import KM._

@RunWith(classOf[JUnitRunner])
class KMeansSuite extends FunSuite {

  def checkClassify(points: GenSeq[Point], means: GenSeq[Point], expected: GenMap[Point, GenSeq[Point]]) {
    assert(classify(points, means) == expected,
      s"classify($points, $means) should equal to $expected")
  }

  test("'classify should work for empty 'points' and empty 'means'") {
    val points: GenSeq[Point] = IndexedSeq()
    val means: GenSeq[Point] = IndexedSeq()
    val expected = GenMap[Point, GenSeq[Point]]()
    checkClassify(points, means, expected)
  }

  test("'classify' should work for empty 'points' and 'means' == GenSeq(Point(1,1,1))") {
    val points: GenSeq[Point] = IndexedSeq()
    val mean = new Point(1, 1, 1)
    val means: GenSeq[Point] = IndexedSeq(mean)
    val expected = GenMap[Point, GenSeq[Point]]((mean, GenSeq()))
    checkClassify(points, means, expected)
  }

  test("'classify' should work for 'points' == GenSeq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == GenSeq((0, 0, 0))") {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean = new Point(0, 0, 0)
    val means: GenSeq[Point] = IndexedSeq(mean)
    val expected = GenMap((mean, GenSeq(p1, p2, p3, p4)))
    checkClassify(points, means, expected)
  }

  test("'classify' should work for 'points' == GenSeq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == GenSeq((1, 0, 0), (-1, 0, 0))") {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean1 = new Point(1, 0, 0)
    val mean2 = new Point(-1, 0, 0)
    val means: GenSeq[Point] = IndexedSeq(mean1, mean2)
    val expected = GenMap((mean1, GenSeq(p1, p2)), (mean2, GenSeq(p3, p4)))
    checkClassify(points, means, expected)
  }

  def checkParClassify(points: GenSeq[Point], means: GenSeq[Point], expected: GenMap[Point, GenSeq[Point]]) {
    assert(classify(points.par, means.par) == expected,
      s"classify($points par, $means par) should equal to $expected")
  }

  test("'classify with data parallelism should work for empty 'points' and empty 'means'") {
    val points: GenSeq[Point] = IndexedSeq()
    val means: GenSeq[Point] = IndexedSeq()
    val expected = GenMap[Point,GenSeq[Point]]()
    checkParClassify(points, means, expected)
  }

  test("converged with the same sets of means") {
    val p1 = new Point(1, 1, 1)
    val p2 = new Point(2, 2, 2)
    val p3 = new Point(3, 3, 3)
    val p4 = new Point(4, 4, 4)
    val oldMeans: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val newMeans: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    assert(converged(0.0)(oldMeans, newMeans) == true)
  }
  
  test("converged with one different point") {
    val p1 = new Point(1, 1, 1)
    val p2 = new Point(2, 2, 2)
    val p3 = new Point(3, 3, 3)
    val p4 = new Point(4, 4, 4)
    val p5 = new Point(4, 4, 4.01)
    val oldMeans: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val newMeans: GenSeq[Point] = IndexedSeq(p1, p2, p3, p5)
    assert(converged(0.001)(oldMeans, newMeans) == true)
  }
  
  test("update") {
    val p0 = new Point(0, 0, 0)
    val p1 = new Point(1, 1, 1)
    val p2 = new Point(2, 2, 2)
    val p3 = new Point(3, 3, 3)
    val p4 = new Point(4, 4, 4)
    val p5 = new Point(0, 0, 0)
    val points: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val oldMeans: GenSeq[Point] = IndexedSeq(p5)
    
    var classified: GenMap[Point, GenSeq[Point]] = GenMap[Point, GenSeq[Point]]()
    classified += (p0 -> points)
    
    val newMeans = update(classified, oldMeans)
    // println(s"$newMeans")
  }
}


  
