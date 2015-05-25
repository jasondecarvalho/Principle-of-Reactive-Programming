package kvstore

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kvstore.Arbiter._
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{FunSpec, BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.util.Random

class Step7_PrimarySecondaryConcertSpec extends TestKit(ActorSystem("Step7PrimarySecondaryConcertSpec"))
with FunSuiteLike
with BeforeAndAfterAll
with Matchers
with ConversionCheckedTripleEquals
with ImplicitSender
with Tools {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  test("case1: Primary and secondaries must work in concert when persistence is unreliable") {
    val flakyPersistence: Boolean = true
    val flakyCommunication: Boolean = false

    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), "case1-primary")
    val user = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val secondary = setUpSecondaries(arbiter, primary, user, flakyPersistence, flakyCommunication, "case1")

    performAndCheckOperations(user, secondary)
  }

  test("case2: Primary and secondaries must work in concert when communication to secondaries is unreliable") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case2-primary")
    val user = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val flakyPersistence: Boolean = false
    val flakyCommunication: Boolean = true

    val secondary = setUpSecondaries(arbiter, primary, user, flakyPersistence, flakyCommunication, "case2")

    performAndCheckOperations(user, secondary)
  }

  test("case3: Primary and secondaries must work in concert when persistence and communication to secondaries is unreliable") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case3-primary")
    val user = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val flakyPersistence: Boolean = true
    val flakyCommunication: Boolean = true

    val secondary = setUpSecondaries(arbiter, primary, user, flakyPersistence, flakyCommunication, "case3")

    performAndCheckOperations(user, secondary)
  }

  private def setUpSecondaries(arbiter: TestProbe,
                               primary: ActorRef,
                               user: Session,
                               flakyPersistence: Boolean,
                               flakyCommunication: Boolean,
                               caseName: String): Session = {
    val secondary1 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), caseName + "-secondary1")
    val secondary2 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), caseName + "-secondary2")
    val secondary3 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), caseName + "-secondary3")
    val flakySecondary1 = system.actorOf(Tools.FlakySecondaryProps(secondary = secondary1, flakyCommunication), caseName + "-flakySecondary1")
    val flakySecondary2 = system.actorOf(Tools.FlakySecondaryProps(secondary = secondary2, flakyCommunication), caseName + "-flakySecondary2")
    val flakySecondary3 = system.actorOf(Tools.FlakySecondaryProps(secondary = secondary3, flakyCommunication), caseName + "-flakySecondary3")

    arbiter.send(secondary1, JoinedSecondary)
    arbiter.send(secondary2, JoinedSecondary)
    arbiter.send(secondary3, JoinedSecondary)

    user.setAcked("k1", "v1")
    arbiter.send(primary, Replicas(Set(primary, flakySecondary1, flakySecondary2, flakySecondary3)))

    session(secondary1)
  }

  private def performAndCheckOperations(user: Session, secondary: Session) = {
    checkInsert(user, "v1")
    user.get("k1") should === (Some("v1"))
    checkInsert(user, "v2")
    user.get("k1") should === (Some("v2"))
    checkInsert(user, "v3")
    user.get("k1") should === (Some("v3"))
    checkRemove(user)
    user.get("k1") should === (None)
    checkInsert(user, "v4")
    user.get("k1") should === (Some("v4"))
    checkRemove(user)
    user.get("k1") should === (None)
  }

  private def checkRemove(user: Session) = {
    val ack = user.remove("k1")
    user.waitAck(ack)
  }

  private def checkInsert(user: Session, value: String) = {
    val ack = user.set("k1", value)
    user.waitAck(ack)
  }

}
