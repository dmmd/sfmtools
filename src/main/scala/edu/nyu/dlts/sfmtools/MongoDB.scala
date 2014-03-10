package edu.nyu.dlts.sfmtools

class MongoDB(){
  import com.mongodb.casbah.Imports._
  import com.mongodb.casbah.commons.MongoDBObjectBuilder
  
  val mongo = MongoClient("localhost", 27017)
  val db = mongo("sfm")
  val items = db("item")
  
  def newBuilder(): MongoDBObjectBuilder = {
    new MongoDBObjectBuilder()
  }

  def getNames(): List[String] = {
    import scala.collection.mutable.ListBuffer
    val names = ListBuffer[String]()
    for(name <- items.distinct("name")){
      names += name.toString()
    }
    names.toList
  }

  def getTextsForName(name: String): MongoCursor = {
    val q = MongoDBObject("name" -> name)
    val fields = MongoDBObject("text" -> 1)
    items.find(q, fields)
  }

  def getHashTags(name: String): Set[String] = {
    import scala.util.matching.Regex
    val pattern = "(\\#\\S* {1}|\\#\\S$)".r    
    val cursor = getTextsForName(name)
    val set = scala.collection.mutable.Set[String]()
    for(item <- cursor){
      (pattern findAllIn item.get("text").toString()).foreach{tag => 
	set += tag
      }
    }
    set.toSet
  }
}


