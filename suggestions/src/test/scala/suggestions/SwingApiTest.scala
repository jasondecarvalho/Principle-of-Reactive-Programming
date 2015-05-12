package suggestions


import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import suggestions.gui._

import scala.collection._
import scala.swing.Reactions.Reaction
import scala.swing.event.Event

@RunWith(classOf[JUnitRunner])
class SwingApiTest extends FunSuite {

  object swingApi extends SwingApi {

    class ValueChanged(val textField: TextField) extends Event

    class ButtonClicked(val source: Button) extends Event

    class Component {
      private val subscriptions = mutable.Set[Reaction]()

      def subscribe(r: Reaction) {
        subscriptions add r
      }

      def unsubscribe(r: Reaction) {
        subscriptions remove r
      }

      def publish(e: Event) {
        for (r <- subscriptions) r(e)
      }
    }

    class TextField extends Component {
      private var _text = ""

      def text = _text

      def text_=(t: String) {
        _text = t
        publish(new ValueChanged(this))
      }
    }

    class Button extends Component {
      def click() {
        publish(new ButtonClicked(this))
      }
    }

    object ValueChanged {
      def unapply(x: Event) = x match {
        case vc: ValueChanged => Some(vc.textField)
        case _ => None
      }
    }

    object ButtonClicked {
      def unapply(x: Event) = x match {
        case bc: ButtonClicked => Some(bc.source)
        case _ => None
      }
    }

  }

  import swingApi._

  test("SwingApi should emit text field values to the observable") {
    val textField = new swingApi.TextField
    val values = textField.textValues

    val observed = mutable.Buffer[String]()
    val sub = values subscribe {
      observed += _
    }

    // write some text now
    textField.text = "T"
    textField.text = "Tu"
    textField.text = "Tur"
    textField.text = "Turi"
    textField.text = "Turin"
    textField.text = "Turing"

    assert(observed == Seq("T", "Tu", "Tur", "Turi", "Turin", "Turing"), observed)
  }

  test("SwingApi should emit button press values to the observable") {
    // Given
    val button1 = new swingApi.Button
    val button2 = new swingApi.Button

    val button1Clicks = button1.clicks
    val button2Clicks = button2.clicks

    val observed = mutable.Buffer[Button]()
    button1Clicks subscribe {
      observed += _
    }
    button2Clicks subscribe {
      observed += _
    }

    // When
    button1.click()
    button2.click()
    button2.click()
    button1.click()

    // Then
    assert(observed == Seq(button1, button2, button2, button1), observed)
  }

}
