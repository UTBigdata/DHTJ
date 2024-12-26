package TraPartitioner

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue, Stack, Set}
//站点为字符串
class SegmentTree extends Serializable{
  import SegmentTree._
  val segments: mutable.Map[String, ArrayBuffer[Segment]] = mutable.Map.empty
  val root = new Segment(null)


  def getTree(sta2nextStas: Map[String, Array[(String, String)]], //Map[(fromSta, Array[(toSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
              basicSeg_line_direc: Array[((String, String), Int, Int)],//Array[(基础段, line, direc)] direc = 0 或 1，代表线路方向（上行 or 下行）
              linedirec2stations: Map[String, Array[String]], //Map[(line_direc, 有序stations)]
              validPathMap: Map[(String, String), Array[String]]): this.type ={
    root.children = basicSeg_line_direc.map(_._1).map(seg => new Segment(root,seg._1, seg._2)).toArray  //所有基础段都是root的孩子节点
    val stack = Stack[Segment]()
    stack.pushAll(root.children.reverse)

    val basicSeg2line = basicSeg_line_direc.map(v=>(v._1,v._2)).toMap
    val processedStrSegment = ArrayBuffer[String]() //避免重复扩展一个段，节省时间和空间
    while(stack.nonEmpty){
      val curr = stack.pop()
      if(segments.contains(curr.toString)) segments(curr.toString) += curr
      else segments += ((curr.toString, ArrayBuffer(curr)))

      printf("The current Segment is: "+ curr.toString+"\n")

      if(curr.numTransfer(basicSeg2line) <= 3 && !(processedStrSegment.contains(curr.toString))){
        printf("Extending path "+ curr.toString+" working ...\n")
        val extendedSegments = extendSegment(curr,sta2nextStas,basicSeg2line,linedirec2stations,validPathMap)
        curr.children = extendedSegments
        stack.pushAll(curr.children.reverse)

        processedStrSegment += curr.toString
        printf("Extending path done of " + processedStrSegment.size+"\n")
      }
    }

    this
  }//end function getTree

  //分别从首尾两个方向扩展一个段
  //只有扩展出来的段属于有效路径，才放到扩展结果中
  /* 问题1：如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失
  解决：保证newSegment.toSta的每一个（换乘）方向都要有一个段，且该段属于有效路径*/
  def extendSegment(seg: Segment,
                    sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
                    basicSeg2line: Map[(String, String), Int],
                    linedirec2stations: Map[String, Array[String]], //Map[(line_direc, 有序stations)]
                    validPathMap: Map[(String, String), Array[String]]): Array[Segment] ={
    val extendedSegments = ArrayBuffer[Segment]()

    /** 基于起点站 fromSta 扩展：找新的起点站,终点站保持不变 */
    val closedNextSta1 = if(seg.middle.size == 0) seg.toSta else seg.middle.head

    val nextStas_fromSta = sta2nextStas(seg.fromSta).filter(_._1 != closedNextSta1) //fromSta middle toSta
    for(nextSta_linedirec <- nextStas_fromSta){
      val newSegment = new Segment(seg)
      newSegment.fromSta = nextSta_linedirec._1 //seg.fromSta的下一个站点变成新段的起点
      newSegment.middle = seg.fromSta +: seg.middle
      newSegment.toSta = seg.toSta
      //只有扩展出来的段属于有效路径，才放到扩展结果中
      if(validPathMap((newSegment.fromSta,newSegment.toSta)).contains(newSegment.toString))
        extendedSegments += newSegment
      else{//但是如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失 所以就必须保证newSegment.fromSta的每一个(换乘)方向都要有一个段，且该段属于有效路径
      val nextLinedirecs = sta2nextStas(newSegment.fromSta).filter(_._1 != newSegment.middle.head).map(_._2)
        for(linedirec <- nextLinedirecs){

          val extendedSegment = extendSeg_linedirec(newSegment,sta2nextStas,basicSeg2line,validPathMap,linedirec,false) //flag为false：扩展起点站
          if(extendedSegment != null) extendedSegments += extendedSegment
          //考虑从当前线路方向到其他线路的换乘
          val segToTransferstas = segToTransfersta_linedirec(newSegment.fromSta,linedirec,sta2nextStas,linedirec2stations)
          segToTransferstas.foreach(segTotfsta_linedirec =>{
            val newSegment1 = newSegment.copy()
            newSegment1.fromSta = segTotfsta_linedirec._1.last
            newSegment1.middle = segTotfsta_linedirec._1.dropRight(1).reverse ++ newSegment1.middle
            val extendedSegment = extendSeg_linedirec(newSegment1,sta2nextStas,basicSeg2line,validPathMap,segTotfsta_linedirec._2,false)//flag为true：扩展起点站
            if(extendedSegment != null) extendedSegments += extendedSegment
          })
        }
      }

    }//end for

    /** 基于终点站 toSta 扩展：找新的终点站,起点站保持不变 */
    //终点站的阻塞站点
    val closedNextSta2 = if(seg.middle.size == 0) seg.fromSta else seg.middle.last //fromSta middle toSta
    //nextStas_toSta: Array[(neighborSta, line_direc)]
    val nextStas_toSta = sta2nextStas(seg.toSta).filter(_._1 != closedNextSta2)
    for(nextSta_linedirec <- nextStas_toSta){
      val newSegment = new Segment(seg)
      newSegment.fromSta = seg.fromSta
      newSegment.middle = seg.middle :+ seg.toSta
      newSegment.toSta = nextSta_linedirec._1

      if(validPathMap((newSegment.fromSta,newSegment.toSta)).contains(newSegment.toString))
        extendedSegments += newSegment
      else{//但是如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失 所以就必须保证newSegment.toSta的每一个方向都要有一个段，且该段属于有效路径
      val nextLinedirecs = sta2nextStas(newSegment.toSta).filter(_._1 != newSegment.middle.last).map(_._2)
        for(linedirec <- nextLinedirecs){

          val extendedSegment = extendSeg_linedirec(newSegment,sta2nextStas,basicSeg2line,validPathMap,linedirec,true)//flag为true：扩展终点站
          if(extendedSegment != null) extendedSegments += extendedSegment

          val segToTransferstas = segToTransfersta_linedirec(newSegment.toSta,linedirec,sta2nextStas,linedirec2stations)
          segToTransferstas.foreach(segTotfsta_linedirec =>{
            val newSegment1 = newSegment.copy()
            newSegment1.toSta = segTotfsta_linedirec._1.last
            newSegment1.middle = newSegment1.middle ++ segTotfsta_linedirec._1.dropRight(1)
            val extendedSegment = extendSeg_linedirec(newSegment1,sta2nextStas,basicSeg2line,validPathMap,segTotfsta_linedirec._2,true)//flag为true：扩展终点站
            if(extendedSegment != null) extendedSegments += extendedSegment
          })
        }//end for
      }//end else

    }//end for

    extendedSegments.toArray
  }//end function extendSegment

  //沿指定的线路方向扩展段，一旦找到一个属于有效路径的扩展段，就返回该扩展段
  def extendSeg_linedirec(seg: Segment,
                          sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
                          basicSeg2line: Map[(String, String), Int], //Map: <基础段，该段所在的地铁线路>  不考虑一个基础段有多条直达路径的情况（2018年的深圳地铁没有这种情况）
                          validPathMap: Map[(String, String), Array[String]],
                          linedirec: String, flag: Boolean): Segment = {
    var extendedSegment: Segment = null

    if(flag){ //flag为true：扩展终点站
      val newSegment1 = seg.copy()
      var tmp = sta2nextStas(newSegment1.toSta).filter(_._2 == linedirec)
      while(!(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)) && tmp.size>0 && newSegment1.numTransfer(basicSeg2line)<=3){//加换乘次数判断是因为seg.toSta的下一个站可能是别的线路上的

      val nnSta = tmp(0)._1
        newSegment1.middle = newSegment1.middle :+ newSegment1.toSta
        newSegment1.toSta = nnSta
        tmp = sta2nextStas(newSegment1.toSta).filter(_._2==linedirec)
      }
      if(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString))
        extendedSegment = newSegment1
    }else{ //flag为false：扩展起点站
      val newSegment1 = seg.copy()
      var tmp = sta2nextStas(newSegment1.fromSta).filter(_._2 == linedirec)
      while(!(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)) && tmp.size>0 && newSegment1.numTransfer(basicSeg2line)<=3){

        val nnSta = tmp(0)._1
        newSegment1.middle = newSegment1.fromSta +: newSegment1.middle
        newSegment1.fromSta = nnSta
        tmp = sta2nextStas(newSegment1.fromSta).filter(_._2 == linedirec)
      }
      if((validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)))
        extendedSegment = newSegment1
    }

    extendedSegment
  }

  //给定站点 sta 和线路方向 linedirec，首先过滤出linedirec 方向上顺序在 sta 之后的换乘站点，然后生成从sta到这些换乘站点的段，以及记录换乘到其他线路的线路方向
  def segToTransfersta_linedirec(sta: String, linedirec: String,
                                 sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])]
                                 linedirec2stations: Map[String, Array[String]] //Map[(line_direc, 有序stations)]
                                ): Array[(List[String], String)]={
    //List[String]: 记录从 sta 沿着 linedirec 方向到 一个换乘站点的路径，String：记录 seg通过该换乘站点换乘到下一条线路的换乘方向
    val res = ArrayBuffer[(List[String], String)]()

    //过滤出 linedirec 方向上的换乘站（相当于这条线路上的所有换乘站点）
    val transferStas = sta2nextStas.toArray.filter(_._2.map(_._2.split("_")(0)).distinct.size>1).//过滤出换乘站
      map(sta_arr => (sta_arr._1,sta_arr._2.filter(_._2 == linedirec))).filter(_._2.size>0).map(_._1) //过滤出 linedirec 方向上的换乘站

    val stations = linedirec2stations(linedirec)
    val indexOfsta = stations.indexOf(sta)
    // linedirec 方向上顺序在 sta 之后的换乘站点
    val transferStas1 =  stations.slice(indexOfsta,stations.size).intersect(transferStas).diff(Array(sta))

    for(transferSta <- transferStas1){
      val indexOftfSta = stations.indexOf(transferSta)
      val seg = stations.slice(indexOfsta,indexOftfSta + 1).toList //用于记录从 sta 沿着 linedirec 方向到 transferSta的路径
      sta2nextStas(transferSta).filter(_._2.split("_")(0) != linedirec.split("_")(0)).map(_._2).foreach(linedirec1 => res += ((seg,linedirec1)) )//记录 seg到下一条线路的换乘方向
    }//end for

    res.toArray
  }

  /** 得到某个字符串段的所有后代节点（包含它自己）*/
  def descendants(strSegment: String): Array[Segment]={
    val results = ArrayBuffer[Segment]()

    //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
    val tmp = segments(strSegment).filter(_.children.size >= 1)
    var targetSeg: Segment = null
    if(tmp.size == 0) //strSeg位于叶子节点级
      results += segments(strSegment)(0)
    else {
      targetSeg = tmp(0)
      val queue = Queue[Segment]()
      queue += targetSeg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr
        queue ++= curr.children
      }
    }
    results.toArray
  }

  /** 得到某个段的所有后代节点（包含它自己）*/
  def descendants(seg: Segment): Array[Segment]={
    val results = ArrayBuffer[Segment]()

    if(isLeafSegment(seg))
      results += seg
    else{
      val queue = Queue[Segment]()
      queue += seg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr
        queue ++= curr.children
      }
    }
    results.toArray
  }

  /** 得到包含某个段的所有段  与descendants相比，要考虑段的某个孩子没有被扩展的情况*/
  //广度优先遍历
  def overlapSegments_BFS(strSegment: String): Array[String]={
    val results = Set[String]()

    if(isLeafSegment(strSegment)) //strSeg位于叶子节点级
      results += strSegment
    else {
      //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
      val targetSeg = segments(strSegment).filter(_.children.size >= 1)(0)
      val queue = Queue[Segment]()
      queue += targetSeg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr.toString
        if(!isLeafSegment(curr) && curr.children.size==0){ //curr为没有被扩展的中间段
          val curr1 = segments(curr.toString).filter(_.children.size >= 1)(0) //找到curr.toString对应的扩展过的那个段
          queue ++= curr1.children
        }
        else
          queue ++= curr.children
      }//end while
    }//end else
    results.toArray
  }//end function

  /** 得到包含某个段的所有段  与descendants相比，要考虑段的某个孩子没有被扩展的情况*/
  def overlapSegments(strSegment: String): Array[String]={
    val results = ArrayBuffer[String]()

    if(isLeafSegment(strSegment)) //strSeg位于叶子节点级
      results += strSegment
    else {
      //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
      val targetSeg = segments(strSegment).filter(_.children.size >= 1)(0)
      val stack = Stack[Segment]()
      stack.push(targetSeg)
      while(stack.nonEmpty){
        val curr = stack.pop()
        results += curr.toString
        if(curr.children.size > 0)
          curr.children.reverse.foreach(seg => stack.push(seg))
        else{ //curr.children.size = 0
          if(!isLeafSegment(curr)){//curr为没有被扩展的中间段
            val curr1 = segments(curr.toString).filter(_.children.size >= 1)(0)
            curr1.children.reverse.foreach(seg => stack.push(seg)) //先压入右节点再压入左节点
          }
        }//end else

      }//end while
    }//end outer else

    results.toArray
  }

  //从叶节点开始，计算包含每个段的所有段
  def overlapSegments(seg2num: Map[String, Int]):this.type ={
    //一个叶子段可以对应多个Segment
    val leafSegments: Array[Segment] = segments.filterKeys(strSeg=>isLeafSegment(strSeg)).values.toArray.flatMap(arr=>arr)
    leafSegments.foreach(seg => seg.overlapSegs = Array(seg2num(seg.toString)) )

    val arr = Set[Segment]()
    arr ++= leafSegments.map(_.parent)
    var flag = true
    while(arr.nonEmpty && flag){
      val tmp = arr.map(seg=>{//过滤出满足条件的段，这些段的所有孩子的重叠段均已求得
        val children = seg.children.map(child =>{
          var child1 = child
          if(!isLeafSegment(child1) && child1.children.size==0) //seg为没有被扩展的中间段
            child1 = segments(child1.toString).filter(_.children.size >= 1)(0) //找到seg.toString对应的扩展过的那个段
          child1
        })
        (seg, children)
      }).filter(_._2.map(_.overlapSegs.size).min>=1)
      tmp.foreach(v => {
        val curr = v._1
        val children = v._2
        curr.overlapSegs = Array(seg2num(curr.toString)) ++ children.flatMap(_.overlapSegs).distinct
        printf("Computating down for Segment: "+ curr.toString+"\n")

        val parents = segments(curr.toString).map(_.parent).filter(p => !p.isRoot)
        arr ++= parents

      })
      if(tmp.size==0) flag = false
      arr --= tmp.map(_._1) //将计算过的段从 arr 中删掉
      printf("The number of remaining Segments: "+ arr.size+"\n")
    }//end while

    this
  }//end func

  def isLeafSegment(seg: Segment):Boolean={
    //是叶子节点：段seg.toString对应的所有Segment都没有被扩展过
    segments(seg.toString).filter(_.children.size >=1).size ==0
  }

  def isLeafSegment(strSeg: String):Boolean={
    //是叶子节点：段seg.toString对应的所有Segment都没有被扩展过
    segments(strSeg).filter(_.children.size >=1).size ==0
  }

}//end class

object SegmentTree{

class Segment(val parent: Segment) extends Serializable{
  var fromSta: String = _
  var toSta: String = _
  var middle = List[String]()  //中间站点集

  var children = Array[Segment]()
  var overlapSegs = Array[Int]() //包含该段的所有段集合

  def this(parent: Segment,fromStation: String, toStation: String){ //用于基础段构建
    this(parent)
    this.fromSta = fromStation
    this.toSta = toStation
  }

  def isRoot: Boolean = parent==null

  def numTransfer(basicSeg2line: Map[(String, String), Int]): Int={
    if(this.isRoot || this.parent.isRoot) return 0
    else{
      var count = 0
      val tmp = this.toString.split("-")
      for(i <- 0 to tmp.size-3)
        if(basicSeg2line(tmp(i),tmp(i+1)) != basicSeg2line(tmp(i+1),tmp(i+2)))
          count += 1
      return count
    }
  }//end function numTransfer

  def copy(): Segment={
    val seg = new Segment(this.parent)
    seg.fromSta = fromSta
    seg.toSta = toSta
    seg.middle = middle
    seg.children = children

    seg
  }

  override def toString: String = {
    if(this.isRoot) ""
    else if(this.middle.size >= 1)
      this.fromSta + "-" + this.middle.mkString("-") + "-" + this.toSta
    else
      this.fromSta + "-" + this.toSta
  }
}//end class Segment

}//end object
