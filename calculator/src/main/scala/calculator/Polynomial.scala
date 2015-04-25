package calculator

import java.lang.Double.{isNaN, isInfinite}

import scala.math.{pow, sqrt}

object Polynomial {

  def computeDelta(a: Signal[Double], b: Signal[Double], c: Signal[Double]): Signal[Double] = {
    Signal(
      pow(b(), 2) - (4 * a() * c())
    )
  }

  def computeSolutions(a: Signal[Double],
                       b: Signal[Double],
                       c: Signal[Double],
                       delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(
      Set(
        (b() * (-1) + sqrt(delta())) / (2 * a()),
        (b() * (-1) - sqrt(delta())) / (2 * a())
      ).filterNot(isNaN).filterNot(isInfinite)
    )
  }
}
