package suggestions
package gui

import javax.swing.UIManager

import rx.lang.scala.{Observable, Subscription}
import suggestions.observablex._
import suggestions.search._

import scala.collection.mutable.ListBuffer
import scala.swing.Orientation._
import scala.swing.Swing._
import scala.swing._
import scala.swing.event._
import scala.util.{Failure, Success, Try}

object WikipediaSuggest extends SimpleSwingApplication with ConcreteSwingApi with ConcreteWikipediaApi {

  {
    try {
      UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName())
    } catch {
      case t: Throwable =>
    }
  }

  def top = new MainFrame {

    /* gui setup */

    title = "Query Wikipedia"
    minimumSize = new Dimension(900, 600)

    val button = new Button("Get") {
      icon = new javax.swing.ImageIcon(javax.imageio.ImageIO.read(this.getClass.getResourceAsStream("/suggestions/wiki-icon.png")))
    }
    val searchTermField = new TextField
    val suggestionList = new ListView(ListBuffer[String]())
    val status = new Label(" ")
    val editorpane = new EditorPane {

      import javax.swing.border._

      border = new EtchedBorder(EtchedBorder.LOWERED)
      editable = false
      peer.setContentType("text/html")
    }

    contents = new BoxPanel(orientation = Vertical) {
      border = EmptyBorder(top = 5, left = 5, bottom = 5, right = 5)
      contents += new BoxPanel(orientation = Horizontal) {
        contents += new BoxPanel(orientation = Vertical) {
          maximumSize = new Dimension(240, 900)
          border = EmptyBorder(top = 10, left = 10, bottom = 10, right = 10)
          contents += new BoxPanel(orientation = Horizontal) {
            maximumSize = new Dimension(640, 30)
            border = EmptyBorder(top = 5, left = 0, bottom = 5, right = 0)
            contents += searchTermField
          }
          contents += new ScrollPane(suggestionList)
          contents += new BorderPanel {
            maximumSize = new Dimension(640, 30)
            add(button, BorderPanel.Position.Center)
          }
        }
        contents += new ScrollPane(editorpane)
      }
      contents += status
    }

    val eventScheduler = SchedulerEx.SwingEventThreadScheduler

    /**
     * Observables
     * You may find the following methods useful when manipulating GUI elements:
     * `myListView.listData = aList` : sets the content of `myListView` to `aList`
     * `myTextField.text = "react"` : sets the content of `myTextField` to "react"
     * `myListView.selection.items` returns a list of selected items from `myListView`
     * `myEditorPane.text = "act"` : sets the content of `myEditorPane` to "act"
     */

    val searchTerms: Observable[String] = searchTermField.textValues

    val suggestions: Observable[Try[List[String]]] =
      searchTerms.sanitized.concatRecovered(wikiSuggestResponseStream(_))

    val suggestionSubscription: Subscription = suggestions.observeOn(eventScheduler) subscribe {
      _ match {
        case Success(list) => suggestionList.listData = list
        case Failure(t) => status.text = t.getMessage
      }
    }

    val selections: Observable[String] = button.clicks.concatMap {
      _ => Observable.just(suggestionList.selection.items: _*)
    }

    val pages: Observable[Try[String]] = selections.sanitized.concatRecovered(wikiPageResponseStream)

    val pageSubscription: Subscription = pages.observeOn(eventScheduler) subscribe {
      _ match {
        case Success(page) => editorpane.text = page
        case Failure(t) => editorpane.text = t.getMessage
      }
    }

  }

}


trait ConcreteWikipediaApi extends WikipediaApi {
  def wikipediaSuggestion(term: String) = Search.wikipediaSuggestion(term)

  def wikipediaPage(term: String) = Search.wikipediaPage(term)
}


trait ConcreteSwingApi extends SwingApi {
  type ValueChanged = scala.swing.event.ValueChanged
  type ButtonClicked = scala.swing.event.ButtonClicked
  type TextField = scala.swing.TextField
  type Button = scala.swing.Button

  object ValueChanged {
    def unapply(x: Event) = x match {
      case vc: ValueChanged => Some(vc.source.asInstanceOf[TextField])
      case _ => None
    }
  }
  object ButtonClicked {
    def unapply(x: Event) = x match {
      case bc: ButtonClicked => Some(bc.source.asInstanceOf[Button])
      case _ => None
    }
  }
}
