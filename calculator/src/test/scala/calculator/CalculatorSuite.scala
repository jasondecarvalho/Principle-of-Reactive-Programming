package calculator

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._

@RunWith(classOf[JUnitRunner])
class CalculatorSuite extends FunSuite with ShouldMatchers {

  test("referencing a variable that is not in the map is an error") {
    val namedExpressions: Map[String, Signal[Expr]] = Map("a" -> Signal(Ref("b")))
    val actual: Map[String, Signal[Double]] = Calculator.computeValues(namedExpressions)
    assert(actual("a").apply().isNaN)
  }

  test("cyclic dependency is an error") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Ref("b")),
      "b" -> Signal(Ref("a"))
    )
    val actual: Map[String, Signal[Double]] = Calculator.computeValues(namedExpressions)
    assert(actual("a").apply().isNaN)
    assert(actual("b").apply().isNaN)
  }

  test("literal") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Literal(1))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(1d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  test("plus") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Plus(Literal(1), Literal(1)))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(2d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  test("minus") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Minus(Literal(1), Literal(1)))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(0d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  test("times") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Times(Literal(2), Literal(2)))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(4d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  test("divide") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Divide(Literal(4), Literal(2)))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(2d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  test("ref") {
    val namedExpressions: Map[String, Signal[Expr]] = Map(
      "a" -> Signal(Ref("b")),
      "b" -> Signal(Literal(1))
    )
    val expected: Map[String, Signal[Double]] = Map(
      "a" -> Signal(1d),
      "b" -> Signal(1d)
    )
    assertSameAfterEvaluation(namedExpressions, expected)
  }

  private def assertSameAfterEvaluation(namedExpressions: Map[String, Signal[Expr]], expected: Map[String, Signal[Double]]): Unit = {
    val actual: Map[String, Signal[Double]] = Calculator.computeValues(namedExpressions)
    assertSame(actual, expected)

    def assertSame[T](actual: Map[String, Signal[T]], expected: Map[String, Signal[T]]): Unit = {
      assert(actual.mapValues(sample) == expected.mapValues(sample))

      def sample[T](signal: Signal[T]): T = {
        signal.apply()
      }
    }
  }
}
