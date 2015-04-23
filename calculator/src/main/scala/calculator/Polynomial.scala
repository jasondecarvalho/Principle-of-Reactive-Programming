package calculator

import scala.math.pow
import scala.math.sqrt

object Polynomial {

  def computeDelta(a: Signal[Double], b: Signal[Double], c: Signal[Double]): Signal[Double] = {
    Signal(pow(b(), 2) - (4 * a() * c()))
  }

  def computeSolutions(a: Signal[Double],
                       b: Signal[Double],
                       c: Signal[Double],
                       delta: Signal[Double]): Signal[Set[Double]] = {
    println((b()*(-1) + sqrt(delta())) / (2 * a()))
    Signal(Set(
      (b()*(-1) + sqrt(delta())) / (2 * a()),
      (b()*(-1) - sqrt(delta())) / (2 * a())
    ))
  }
}
