import actors.Actor
import scala.Array
import scala.Array._
import java.lang.Math

/**
 * Created with IntelliJ IDEA.
 * User: KS
 * Date: 10/14/12
 * Time: 6:40 AM
 * To change this template use File | Settings | File Templates.
 */
case class Deliver(msg: String, key: Int)
case class Forward(msg: String, key: Int)
case class InitiateRoutingTable(nodeIds: Array[Int])

object Proj {

  var numNodes, numRequests = 0

  def main(args: Array[String]) {
    collectInput
    pastryInit
  }

  def collectInput() {
    println("Enter Num nodes")
    numNodes = readInt()
    println("Enter Num requests")
    numRequests = readInt()
  }

  def pastryInit() {
    for (i <- 1 to numNodes){
       P2PNetwork.join(new Node())
    }
    P2PNetwork.initiateRoutingTable
  }

}

object P2PNetwork{
  var nodes: Array[Node] = Array()
  var nodeIds: Array[Int] = Array()

  def join(node: Node){
    nodes:+= node
    node.nodeId = nodes.size
    nodeIds:+= nodes.size
    node.start()
  }

  def initiateRoutingTable(){
    for (node <- nodes)
      node ! InitiateRoutingTable(nodeIds)
  }
}

class Node extends Actor{
  var nodeId = -1
  var routingTable: RoutingTable = _

  def act(){
    loop {
      react{
        case InitiateRoutingTable(nodeIds: Array[Int]) => initiateRoutingTable(nodeIds)
      }
    }
  }

  def initiateRoutingTable(nodeIds: Array[Int]){
    routingTable = new RoutingTable(P2PNetwork.nodes.size)
    if (nodeId != -1){
      initLeafSet
      initRoutingDetails
      initNeighborSet // Not Needed for Now
      // code to find leafset bigger and smaller
    }

  }

}

class RoutingTable(nodeSize: Int){
  val b = 4 // Set for now
  val rowSize = Math.floor(Math.log(nodeSize.toDouble)/Math.log(b.toDouble)).toInt
  val columnSize, leafSetSize = (Math.pow(2,b)).toInt

  var smallLeafSet: Array[Int]= Array()
  var bigLeafSet: Array[Int]= Array()
  var neighborSet: Array[Int]= Array()
  var routingDetails = ofDim[Int](rowSize, (Math.pow(2,b)).toInt)

  def initLeafSet(callerId: Int, nodeIds: Array[Int]){
    scala.util.Sorting.quickSort(nodeIds)
    var startIndex, endIndex = -1
    for (n <- 0 to nodeIds.size - 1){
      if (nodeIds(n) == callerId){
        if (n > 0){
          endIndex = n - 1
          if (n - leafSetSize/2 < 0)
            startIndex = 0
          for (i <- startIndex to endIndex)
            smallLeafSet:+= nodeIds(i)
        }
        if (n < nodeIds.size - 1){
          startIndex = n + 1
          if (n + leafSetSize/2 > nodeIds.size - 1)
            endIndex = nodeIds.size - 1
          for (i <- startIndex to endIndex)
            bigLeafSet:+= nodeIds(i)
        }
      }
    }
  }

}
