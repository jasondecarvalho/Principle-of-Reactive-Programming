package calculator

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, _}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PolynomialSuite extends FunSuite with ShouldMatchers {

  test("calculate determinant with a constant signal") {
    val a = Signal(2d)
    val b = Signal(2d)
    val c = Signal(2d)

    val delta: Double = Polynomial.computeDelta(a, b, c)()
    assert(delta == -12)
  }

  test("calculate solutions with a constant signal") {
    val a = Signal(1d)
    val b = Signal(0d)
    val c = Signal(-16d)

    val delta: Signal[Double] = Polynomial.computeDelta(a, b, c)
    val solutions: Set[Double] = Polynomial.computeSolutions(a, b, c, delta)()
    assert(solutions == Set(4, -4))
  }

  test("single solution is a singleton set") {
    val a = Signal(1d)
    val b = Signal(-8d)
    val c = Signal(16d)

    val delta: Signal[Double] = Polynomial.computeDelta(a, b, c)
    val solutions: Set[Double] = Polynomial.computeSolutions(a, b, c, delta)()
    assert(solutions == Set(4))
  }

  test("no solutions is the empty set") {
    val a = Signal(1d)
    val b = Signal(0d)
    val c = Signal(16d)

    val delta: Signal[Double] = Polynomial.computeDelta(a, b, c)
    val solutions: Set[Double] = Polynomial.computeSolutions(a, b, c, delta)()
    assert(solutions == Set())
  }

  test("signal updates are propagated") {
    val a = Var(1d)
    val b = Var(0d)
    val c = Var(16d)

    val delta: Signal[Double] = Polynomial.computeDelta(a, b, c)
    val solutions: Signal[Set[Double]] = Polynomial.computeSolutions(a, b, c, delta)

    assert(solutions() == Set())

    a.update(1d)
    b.update(0d)
    c.update(-16d)

    assert(solutions() == Set(4, -4))
  }

}
