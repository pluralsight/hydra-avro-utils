package hydra.avro.util

import java.sql.Connection

/**
  * Created by alexsilva on 7/13/17.
  */
trait JdbcHelper {

  def withConnection[T](conn: => Connection)(body: (Connection) => T): T = synchronized {
    try {
      body(conn)
    }
    finally {
      conn.close()
    }
  }
}
