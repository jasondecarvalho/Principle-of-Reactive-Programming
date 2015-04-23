package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {

  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    def evaluate(exprSignal: Signal[Expr]): Signal[Double] = {
      Signal(eval(exprSignal(), namedExpressions))
    }
    namedExpressions.mapValues(evaluate)
  }

  def eval(expression: Expr, references: Map[String, Signal[Expr]]): Double = {
    evalHelper(expression, references, Set())
  }

  def evalHelper(expression: Expr, references: Map[String, Signal[Expr]], refs: Set[Ref]): Double = {
    expression match {
      case literal: Literal => literal.v
      case plus: Plus => evalHelper(plus.a, references, refs) + evalHelper(plus.b, references, refs)
      case minus: Minus => evalHelper(minus.a, references, refs) - evalHelper(minus.b, references, refs)
      case times: Times => evalHelper(times.a, references, refs) * evalHelper(times.b, references, refs)
      case divide: Divide => evalHelper(divide.a, references, refs) / evalHelper(divide.b, references, refs)
      case ref: Ref =>
        if (refs contains ref) {
          Double.NaN
        } else {
          evalHelper(getReferenceExpr(ref.name, references), references, refs + ref)
        }
    }
  }

  /** Get the Expr for a referenced variable.
    * If the variable is not known, returns a literal NaN.
    */
  private def getReferenceExpr(name: String, references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
