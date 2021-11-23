// Databricks notebook source
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.control.NonFatal

// COMMAND ----------

case class NotebookData(path: String, timeout: Int, parameters: Map[String, String] = Map.empty[String, String])

def parallelNotebooks(notebooks: Seq[NotebookData]): Future[Seq[String]] = {
  import scala.concurrent.{Future, blocking, Await}
  import java.util.concurrent.Executors
  import scala.concurrent.ExecutionContext
  import com.databricks.WorkflowException

  val numNotebooksInParallel = 4 
  // If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
  // This code limits the number of parallel notebooks.
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numNotebooksInParallel))
  val ctx = dbutils.notebook.getContext()
  
  Future.sequence(
    notebooks.map { notebook => 
      Future {
        dbutils.notebook.setContext(ctx)
        if (notebook.parameters.nonEmpty)
          dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
        else
          dbutils.notebook.run(notebook.path, notebook.timeout)
      }
      .recover {
        case NonFatal(e) => s"ERROR: ${e.getMessage}"
      }
    }
  )
}

def parallelNotebook(notebook: NotebookData): Future[String] = {
  import scala.concurrent.{Future, blocking, Await}
  import java.util.concurrent.Executors
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.databricks.WorkflowException

  val ctx = dbutils.notebook.getContext()
  // The simplest interface we can have but doesn't
  // have protection for submitting to many notebooks in parallel at once
  Future {
    dbutils.notebook.setContext(ctx)
    
    if (notebook.parameters.nonEmpty)
      dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
    else
      dbutils.notebook.run(notebook.path, notebook.timeout)
    
  }
  .recover {
    case NonFatal(e) => s"ERROR: ${e.getMessage}"
  }
}

