package com

import scalaz._, Scalaz._
import scalaz.concurrent._

package object example {
  type IntState[A] = State[Int, A]
  type TaskState[A] = StateT[Task, Int, A]
  def liftStateTrans[A](stateTrans: Int => (Int, A)): TaskState[A] = StateT[Task, Int, A](s => Task.now(stateTrans(s)))
  def liftState[A](state: IntState[A]): TaskState[A] = StateT[Task, Int, A](s => Task.now(state.run(s)))
  def liftTask[A](task: Task[A]): TaskState[A] = StateT[Task, Int, A](s => task.map(a => (s, a)))


}
