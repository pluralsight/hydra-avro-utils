package hydra.avro.util

import java.util.Properties

import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 7/13/17.
  */
object ConfigUtils {

  implicit def toProperties(jdbcConfig: Config): Properties = {
    val properties = new Properties
    jdbcConfig.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), jdbcConfig.getString(e.getKey())))
    properties
  }
}
