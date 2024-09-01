//Author: Eric
//Collaborator: Jubrial 
import scala.math.sqrt
import scala.math.log
case class Neumaier(sum: Double, c: Double)

object HW {
    //tabulate funciton simulates a list, used x+1 so 0 is not squared since we don't want the first one
    def q1(n: Int):List[Int] = {
       List.tabulate(n)(x => (x+1) * (x+1))
    }
    //Essentially the same as q1 except input is a vector instead of a list, and we square rooted all the numbers leading to the sqrt import
    def q2(n: Int):Vector[Double] = {
        Vector.tabulate(n)(x => sqrt(x+1))
    }
    //fold left to iterate through all of the elements
    //want to end off with a sum of type double so initalize to 0.0
    //declare total and x, x is the element we are on total is the reoccuring value
    def q3(x: Seq[Double]): Double = {
        x.foldLeft(0.0){(total, x) => total + x}
    }
    //Same concept as 3 except looking for product
    def q4(x: Seq[Double]): Double = {
        x.foldLeft(1.0){(total, x) => total * x}
    }
    //same concept as 3 except sum of logs, imported log 
    def q5(x: Seq[Double]): Double = {
        x.foldLeft(0.0){(total, x) => total + log(x)}
    }
    //used state because we want to add all the elements in the list
    //used ._1 and ._2 b/c they index the position, needed so know we are keeping track of each sequence respectively 
    def q6(x: Seq[(Double, Double)]): (Double, Double) = {
        x.foldLeft(0.0,0.0){(state,add) => (state._1 + add._1, state._2 + add._2)}
    }
    //Set sum to 0.0 for the first items, set max to negative infinity bc max could be a negative number
    //add first items to first state element, set second element of state to the max
    def q7(x: Seq[(Double, Double)]): (Double, Double) = {
        x.foldLeft(0.0,Double.NegativeInfinity){(state, add) => (state._1 + add._1, math.max(state._2, add._2))}
    }
    //used "to" function to simulate range of n numbers starting at 1
    //initalize both values to Int
    //element 1 is the sum, element 2 is the product
    def q8(x: Int): (Int,Int) = {
        (1 to x).foldLeft(0, 1){(state, combine) => (state._1 + combine, state._2 * combine)}
    }
    //map squares all the elements
    //filter removes all of the elements that are not even
    //reduce sums all of the elements
    def q9(x: Seq[Int]): Int = {
        val square = x.map{x => x * x}
        val even = square.filter{x => (x % 2 == 0)}
        val sum = even.reduce{(x,y) => x + y}
        sum
    }
    //element 1 for adding sum, element 2 for incrementing to next element to multiple by 
    //i.e element 1 is the total sum, sum is the current element of the list value, and element 2 is the summation to multiply 1, 2, 3...
    // Add ._1 to return the sum (float) and not our iterator
    def q10(x: Seq[Double]): Double = {
       x.foldLeft(0.0,1.0){(state, sum) => (state._1 + (sum * state._2), state._2 + 1)}._1
    }
    def q11(x: Seq[Double]): Double = {
        1.0
    }
}
