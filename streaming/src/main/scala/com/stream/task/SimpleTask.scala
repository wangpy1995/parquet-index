package com.stream.task

sealed abstract class Task

case class Single[T, U](x: T, func: (T, U) => U) extends Task

case class MultiTask(tasks:Seq[Task]) extends Task