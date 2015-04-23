package calculator

import scala.math.pow
import scala.math.sqrt
import java.lang.Double.isNaN

object Polynomial {

  def computeDelta(a: Signal[Double], b: Signal[Double], c: Signal[Double]): Signal[Double] = {
    Signal(pow(b(), 2) - (4 * a() * c()))
  }

  def computeSolutions(a: Signal[Double],
                       b: Signal[Double],
                       c: Signal[Double],
                       delta: Signal[Double]): Signal[Set[Double]] = {
    val solutions: Set[Double] = Set(
      (b() * (-1) + sqrt(delta())) / (2 * a()),
      (b() * (-1) - sqrt(delta())) / (2 * a())
    ).filterNot(isNaN)
    Signal(solutions)
  }
}
