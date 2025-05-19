package fromurl

import scala.io.Source

object webapi_obj {
  
  def fetchurl(url: String):String ={
     Source.fromURL(url).mkString
    
  }
}