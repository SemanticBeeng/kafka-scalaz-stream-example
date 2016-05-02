package com.example

import scalaz.concurrent._

object Run {
  def main(args: Array[String]): Unit = {
    safeMain(args).unsafePerformSync
  }

  def safeMain(args: Array[String]): Task[Unit] = {

    args.headOption.map(chooseMain).getOrElse {
      Task.delay {
        throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument")
      }
    }
  }

  def chooseMain(arg: String): Task[Unit] = arg match {
    case "producer" =>
      Producer.safeMain
    case "consumer" =>
      Consumer.safeMain
    case _ =>
      Task.delay {
        throw new IllegalArgumentException("Don't know how to do " + arg)
      }
  }
}
