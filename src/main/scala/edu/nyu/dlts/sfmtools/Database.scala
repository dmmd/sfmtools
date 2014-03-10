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

