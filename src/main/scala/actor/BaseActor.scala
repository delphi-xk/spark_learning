package actor

import akka.actor.{Actor, ActorSystem, Props}


/**
  * Created by xk on 2018/3/16.
  */
private[actor] class BaseActor extends Actor{
  override def preStart(): Unit = {
    println("base actor started...")
  }

  override def receive: Receive = {
    case "Hello" => println("hello, too")
    case "close" => context.stop(self)
    case "something failed" => println("actor failed!"); throw new Exception("actor failed!")
    case str => println(s"base actor say: hello $str")

  }

  override def postStop(): Unit = {
    println(s"${self.path} post stopped!")
  }

}

object Main extends App {
  val system  = ActorSystem("akkaSystem")
  val helloActor = system.actorOf(Props[BaseActor], "hello_actor")
  helloActor ! "Hello"
  helloActor ! "xk"
  helloActor ! "something failed"
  helloActor ! "close"

}
