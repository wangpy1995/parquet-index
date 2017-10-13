package com.stream.task

sealed abstract class Task

case class SingleTask[T, U](x: T, func: (T, U) => U) extends Task