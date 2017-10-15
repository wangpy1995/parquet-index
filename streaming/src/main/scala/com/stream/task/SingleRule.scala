package com.stream.task

abstract class Rule

final class AtomRuleNode[T](val name: String, val value: T) {
  override def equals(obj: scala.Any): Boolean =obj match {
    case that: AtomRuleNode[T] =>
      this.name == that.name && this.value == that.value
    case _ => super.equals(obj)
  }
}

case class SingleRule(key: Int, factors: List[AtomRuleNode[_]]) extends Rule {
  if (key == -1) assert(factors.length < 2)

  override def equals(obj: scala.Any): Boolean = obj match {
    case SingleRule(thatKey, thatFactors) =>
      if (factors.isEmpty && thatFactors.isEmpty && key == thatKey)
        true
      else
        key == thatKey &&
          factors.length == thatFactors.length &&
          factors.zipWithIndex.map {
            case (factor, i) => thatFactors(i) == factor
          }.reduce(_ && _)
    case _ => super.equals(obj)
  }
}

case class MultiRule(key: Int, tasks: List[Rule]) extends Rule{
  assert(key != -1)
}