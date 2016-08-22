package gaia

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.combinator.syntactical._

abstract class Expr {
  def rpn:String
}
case class BinaryOperator(lhs:Expr, op:String, rhs:Expr) extends Expr {
  def rpn:String = lhs.rpn + " " + rhs.rpn + " " + op
}
case class Number(v:String) extends Expr { def rpn:String = v }
case class Variable(v:String) extends Expr { def rpn:String = v }
case class Function(f:String, e:List[Expr]) extends Expr { def rpn:String = {
  var s = ""
  e.foreach { x => s += x.rpn + " " }
  s += f
  return s
}
}
object ShuntingYard extends StandardTokenParsers {
  lexical.delimiters ++= List("+","-","*","/", "^","(",")",",")

  def value :Parser[Expr] = numericLit ^^ { s => Number(s) }
  def variable:Parser[Expr] =  ident ^^ { s => Variable(s) }
  def parens:Parser[Expr] = "(" ~> expr <~ ")"

  def argument:Parser[Expr] = expr <~ (","?)
  def func:Parser[Expr] = ( ident ~ "(" ~ (argument+) ~ ")" ^^ { case f ~ _ ~ e ~ _ => Function(f, e) })

  def term = (value | parens | func | variable)

  // Needed to define recursive because ^ is right-associative
  def pow :Parser[Expr] = ( term ~ "^" ~ pow ^^ {case left ~ _ ~ right => BinaryOperator(left, "^", right) }|
    term)
  def factor = pow * ("*" ^^^ { (left:Expr, right:Expr) => BinaryOperator(left, "*", right) } |
    "/" ^^^ { (left:Expr, right:Expr) => BinaryOperator(left, "/", right) } )
  def sum =  factor * ("+" ^^^ { (left:Expr, right:Expr) => BinaryOperator(left, "+", right) } |
    "-" ^^^ { (left:Expr, right:Expr) => BinaryOperator(left, "-", right) } )
  def expr = ( sum | term )

  def parse(s:String) = {
    val tokens = new lexical.Scanner(s)
    phrase(expr)(tokens)
  }

  def shunt(exprstr: String) : String = exprstr match {
    case null => return ""
    case "" => return ""
    case _ =>
      parse(exprstr) match {
        case Success(tree, _) =>
          println("Tree: "+tree)
          val v = tree.rpn
          println("RPN: "+v)
          return v
        case e: NoSuccess => Console.err.println(e)
          return e.toString
      }
  }

  def main(args: Array[String]) : Unit = {
    ShuntingYard.shunt(args(0))

  }

}


