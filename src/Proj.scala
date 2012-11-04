import actors.Actor
import collection.immutable.HashMap
import scala.Array
import scala.Array._
import scala.util.Random
import java.lang.Math

/**
 * Created with IntelliJ IDEA.
 * User: KS
 * Date: 10/14/12
 * Time: 6:40 AM
 * To change this template use File | Settings | File Templates.
 */
case class Deliver(msg: Message)
case class Forward(msg: Message, key: Int)
case class InitiateRoutingTable(nodeIds: List[Int])
case class MakeRequests(numRequests: Int)
case class DoRouting(message: Message, nodeId: Int)
case class RemindMe(requestsMade: Int)
case object ThresholdReached
case class MessageRecieved(hopsTravelled: Int)

object GlobalConstants{
  val NODE_ID_LENGTH = 8
  val B = 2
}

class Message(id: Int){
  val t = "Hello!!"
  var hopsTravelled = 0;
}

object Proj {
  var numNodes, numRequests = 0

  def main(args: Array[String]) {
    collectInput
    pastryInit()
  }

  def collectInput() {
    println("Enter Num nodes")
    numNodes = readInt()
    println("Enter Num requests")
    numRequests = readInt()
  }

  def pastryInit() {
    P2PNetwork.start()
    for (i <- 1 to numNodes){
       P2PNetwork.join(new Node())
    }
    println("Joined")
    P2PNetwork.initiateRoutingTable
    P2PNetwork.generateFailure()
    println("Initiated")
    P2PNetwork.startRouting(numRequests)

  }

}

object P2PNetwork extends Actor{
  var nodes: Array[Node] = Array()
  var nodeIds: List[Int] = List()
  var nodeMaps = new HashMap[Int, Node]
  var idLength = GlobalConstants.NODE_ID_LENGTH; //ids will be like 3201, 2100, 0223

  var tiredNodes = 0
  var deliveredMessages = 0
  var maxRequests = 0
  var totalHopsTraveled = 0

  def act(){
    loop {
      react{
        case ThresholdReached => {
          tiredNodes +=1
          if (tiredNodes == nodeIds.length){
            println("All messages were Delieverd with average hop traversals of "+ (totalHopsTraveled/deliveredMessages))
            System.exit(0)
          }
        }
        case MessageRecieved(hopsTravelled: Int) => {
          deliveredMessages +=1
          totalHopsTraveled += hopsTravelled
        }
      }
    }
  }

  def join(node: Node){
    nodes:+= node
    var id = generateId()
    node.nodeId = id
    nodeIds = id :: nodeIds
    nodeMaps += (id -> node)
    node.start()
  }

  def generateFailure(){
    nodeMaps(Random.shuffle(nodeIds).head).isAlive = false
  }

  def initiateRoutingTable(){
    for (node <- nodes){
      node ! InitiateRoutingTable(nodeIds)
    }
  }

  def startRouting(numRequests: Int){
    maxRequests = numRequests
    for (node <- nodes){
      node ! MakeRequests(numRequests)
    }
  }

  /*def thresholdReached(){
    print("Inside threshold")
    tiredNodes +=1
    if (tiredNodes == nodes.length)
      System.exit(0)
  } */

  def generateId() : Int = {
    var id = ""
    do{
      id = ""
      for (i <- 0 to idLength - 1)
        id = id + (new util.Random().nextInt(idLength)).toString
    }while(nodeIds.contains(id.toInt))
    id.toInt
  }
}

class Node extends Actor{
  var nodeId = -1
  var routingTable: RoutingTable = _
  var maxRequests = 0
  var isAlive = true
  var Done = 0

  def act(){
    loop {
      react{
        case InitiateRoutingTable(nodeIds: List[Int]) => initiateRoutingTable(nodeIds)
        case MakeRequests(numRequests: Int) => makeRequests(numRequests)
        case DoRouting(message: Message, destNodeId: Int) => doRouting(message, destNodeId)
        case Deliver(message: Message) => {P2PNetwork ! MessageRecieved(message.hopsTravelled)}
        case RemindMe(requestsMade: Int) => remindMe(requestsMade)
}
}
}

def initiateRoutingTable(nodeIds: List[Int]){
  routingTable = new RoutingTable(P2PNetwork.nodes.size)
  if (nodeId != -1){
    routingTable.initLeafSet(nodeId, nodeIds)
    /*println("My leafset - " + nodeId)
for (i <- routingTable.smallLeafSet)
  print(i + " ")
println()
for (i <- routingTable.bigLeafSet)
  print(i+ " ")*/
    routingTable.initRoutingTable(nodeId, nodeIds)
    //for (row <- routingTable.routingDetails)
      //for (ele <- row)
        //println("my node - "+ nodeId + " - "+ ele)
    //initNeighborSet // Not Needed for Now
    // code to find leafset bigger and smaller
  }

}

def makeRequests(numRequests: Int){
  maxRequests = numRequests
  P2PNetwork.nodeMaps(nodeId) ! RemindMe(0)
}

def remindMe(requestsMade: Int){
  var destNode = selectNode()
  P2PNetwork.nodeMaps(nodeId) ! DoRouting(new Message(nodeId), destNode)
  if (requestsMade == maxRequests){
    P2PNetwork ! ThresholdReached
  }
  else{
    Thread.sleep(1000)
    P2PNetwork.nodeMaps(nodeId) ! RemindMe(requestsMade + 1)
  }
}

def selectNode(): Int = {
  var list = P2PNetwork.nodeIds
  var destNode = 0
  do{
    list = Random.shuffle(list)
    destNode = list.head
  }while(nodeId == destNode)
  destNode
}

def doRouting(message: Message, destNodeId: Int){
  message.hopsTravelled +=1
  var sent = false
  //if (message.hopsTravelled == (10*Math.ceil(Math.log(P2PNetwork.nodeIds.length.toDouble)/Math.log(Math.pow(2,GlobalConstants.B.toDouble).toDouble)))){
    //println("Cant find !!" + message.hopsTravelled + " "+ destNodeId)
    //return
  //}
  //checkInLeafSet
  if (destNodeId < nodeId){
    for (id <- routingTable.smallLeafSet){
      if (id == destNodeId){
        if (!P2PNetwork.nodeMaps(destNodeId).isAlive){
          println("The destination is the failed node and message cannot be deliverd")
          return
        }
        P2PNetwork.nodeMaps(destNodeId) ! Deliver(message)
        sent = true
      }
    }
  }
  else{
    for (id <- routingTable.bigLeafSet){
      if (id == destNodeId){
        if (!P2PNetwork.nodeMaps(destNodeId).isAlive){
          println("The destination is the failed node and message cannot be deliverd")
          return
        }
        P2PNetwork.nodeMaps(destNodeId) ! Deliver(message)
        sent = true
      }
    }
  }
  if (!sent){
    var row = routingTable.findCommonLetters(nodeId, destNodeId)
    if (row == GlobalConstants.NODE_ID_LENGTH){
      P2PNetwork.nodeMaps(nodeId) ! Deliver(message)
      return
    }
    var column = routingTable.makeStr(nodeId).charAt(row).toString.toInt
    //println(row + " - "+ column)
    var nextHop = routingTable.routingDetails(row)(column)
    if (nextHop == destNodeId){
      if (!P2PNetwork.nodeMaps(destNodeId).isAlive){
        println("The destination is the failed node and message cannot be deliverd")
        return
      }
      P2PNetwork.nodeMaps(destNodeId) ! Deliver(message)
    }
    else {
      if (nextHop == -1 || !P2PNetwork.nodeMaps(nextHop).isAlive){
        if (nextHop != -1){
          println("Next Hop is the failed node and an alternate path is being found")
          return
        }
        var count = 0
        do{
          nextHop = findNextHop(row,column,nextHop,destNodeId,count)
          count +=1
        }while(nextHop == -1 || (!P2PNetwork.nodeMaps(nextHop).isAlive) )
      }
      P2PNetwork.nodeMaps(nextHop) ! DoRouting(message, destNodeId)
    }

  }
}

def findNextHop(row:Int,column:Int,nextHop:Int,destNodeId:Int, threshold: Int) : Int = {
  var listRow = List(destNodeId)
  if(threshold == 5)
    return destNodeId
  for (i<- routingTable.routingDetails(row))
    listRow = i :: listRow
  for (i<-routingTable.smallLeafSet)
    listRow = i :: listRow
  for (i<-routingTable.bigLeafSet)
    listRow = i :: listRow
  listRow = listRow.distinct
  var listArray:Array[Int] = listRow.toArray
  scala.util.Sorting.quickSort(listArray)
  for (i <- 0 to listArray.length-1){
    if (listArray(i) == destNodeId){
     if (i != 0 && i!= listArray.length-1){
      if (Math.abs(listArray(i-1)-destNodeId) < Math.abs(listArray(i+1)-destNodeId))
        return listArray(i-1)
      else
        return listArray(i+1)
     }
     else if (i==0)
       return listArray(1)
     else
       return listArray(listArray.length-2)
    }
  }
  return 0;
}
}

class RoutingTable(nodeSize: Int){
  val b = GlobalConstants.B // Set for now
  //val rowSize = Math.ceil(Math.log(nodeSize.toDouble)/Math.log((Math.pow(2,b)).toDouble)).toInt
  val rowSize = Math.max((Math.pow(2,b)).toInt - 1, GlobalConstants.NODE_ID_LENGTH)
  val columnSize = Math.max((Math.pow(2,b)).toInt - 1, GlobalConstants.NODE_ID_LENGTH)
  val leafSetSize = (Math.pow(2,b)).toInt

  var smallLeafSet: Array[Int]= Array()
  var bigLeafSet: Array[Int]= Array()
  var neighborSet: Array[Int]= Array()
  var routingDetails = ofDim[Int](rowSize, columnSize)
  var elementsInRow: Array[Int] = Array.ofDim(rowSize) // This will hold how many values are filled in a row of routing table. when it reaches columnsize break

  def initLeafSet(callerId: Int, nodeIdsList: List[Int]){
    var nodeIds = nodeIdsList.toArray
    scala.util.Sorting.quickSort(nodeIds)
    var startIndex, endIndex = -1
    for (n <- 0 to nodeIds.size - 1){
      if (nodeIds(n) == callerId){
        if (n > 0){
          endIndex = n - 1
          if (n - leafSetSize/2 < 0)
            startIndex = 0
          else
            startIndex = n - leafSetSize/2
          for (i <- startIndex to endIndex)
            smallLeafSet:+= nodeIds(i)
        }
        if (n < nodeIds.size - 1){
          startIndex = n + 1
          if (n + leafSetSize/2 > nodeIds.size - 1)
            endIndex = nodeIds.size - 1
          else
            endIndex = n + leafSetSize/2
          for (i <- startIndex to endIndex)
            bigLeafSet:+= nodeIds(i)
        }
      }
    }
  }

  def initRoutingTable(callerId : Int, nodeIdList: List[Int]){
    var nodeIds = nodeIdList.toArray
    var commonLetters:Int = -1;
    clearRoutingDetails
    for (nodeId <- nodeIds){
      if(nodeId != callerId && !presentInLeafset(nodeId)){
        commonLetters = findCommonLetters(callerId, nodeId)
        if (elementsInRow(commonLetters) < columnSize){
          routingDetails(commonLetters)(elementsInRow(commonLetters)) =  nodeId
          elementsInRow(commonLetters)  += 1
        }
      }
    }
  }

  def clearRoutingDetails{
    for (rows <- 0 to rowSize - 1)
      for (cols <- 0 to columnSize - 1)
        routingDetails(rows)(cols) = -1
  }

  def presentInLeafset(nodeId: Int) : Boolean = {
    for (node <- smallLeafSet){
      if (nodeId == node)
        return true
    }
    for (node <- bigLeafSet){
      if (nodeId == node)
        return true
    }
    false
  }

  def findCommonLetters(callerId: Int, nodeId: Int): Int = {
    var callerIdStr = makeStr(callerId)
    var nodeIdStr = makeStr(nodeId)
    var commonLetters = 0
    for (index <- 0 to callerIdStr.length - 1){
      if (callerIdStr.charAt(index) == nodeIdStr.charAt(index))
        commonLetters +=1
    }
    return commonLetters;
  }

  def makeStr(num : Int): String = {
    var numStr = num.toString
    while(numStr.length < GlobalConstants.NODE_ID_LENGTH)
      numStr = "0" + numStr
    numStr
  }
}
