package edu.nyu.dlts.sfmtools

class Database(){
  import java.sql.{DriverManager}
  import com.typesafe.config._

  val conf = ConfigFactory.load()
  val con = DriverManager.getConnection(conf.getString("postgres.url") + ":" + conf.getString("postgres.port") + "/" + conf.getString("postgres.db"), "sfm", "sfm")

  def loadTweets(mongo: MongoDB): Unit = {
    val statement = con.createStatement()
    val rs = statement.executeQuery("SELECT * FROM ui_twitteruseritem i, ui_twitteruser u WHERE i.twitter_user_id = u.id ")

    while(rs.next()){
      val builder = mongo.newBuilder
      builder += "id" -> rs.getLong("twitter_id")
      builder += "name" -> rs.getString("name")
      builder += "timestamp" -> rs.getTimestamp("date_published")
      builder += "text" -> rs.getString("item_text")
      mongo.items.save(builder.result())
    }
    statement.close
  }
}

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


