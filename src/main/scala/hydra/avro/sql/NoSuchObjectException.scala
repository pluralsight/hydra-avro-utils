package hydra.avro.sql

/**
  * Created by alexsilva on 7/11/17.
  */
class NoSuchDatabaseException(db: String) extends AnalysisException(s"Database '$db' not found")

class NoSuchTableException(db: String, table: String)
  extends AnalysisException(s"Table or view '$table' not found in database '$db'")