package com.xiaoju

import org.json4s._
import org.json4s.jackson.{JsonMethods, Serialization}

object JsonUtils {
  implicit val formats = Serialization.formats(NoTypeHints)

  def toJson[T <: scala.AnyRef](obj: T): String = {
    Serialization.write(obj)
  }

  def parseJson[T: Manifest](json: String) = {
    Serialization.read[T](json)
  }

  def toJObject(json: String): JValue = {
    JsonMethods.parse(new StringInput(json))
  }
}
