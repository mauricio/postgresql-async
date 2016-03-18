package com.github.mauricio.async.db.postgresql.exceptions

import com.github.mauricio.async.db.exceptions.DatabaseException

class PendingCloseStatementException(message : String)
  extends DatabaseException(message)
