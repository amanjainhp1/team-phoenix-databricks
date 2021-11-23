// Databricks notebook source
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

def callStoredProcedure (dbType: String, url: String, port: String, options: String, username: String, password: String, storedProcedureQuery: String) {
  
  var conn: Connection = null
  conn = DriverManager.getConnection(s"""jdbc:${dbType}://${url}:${port}${options}""", username, password)
  if (conn != null) {
      print(s"""Connected to ${dbType}\n""")
  }

  val statement: Statement = conn.createStatement()
  val query: String = storedProcedureQuery
  
  statement.executeUpdate(query)
}
