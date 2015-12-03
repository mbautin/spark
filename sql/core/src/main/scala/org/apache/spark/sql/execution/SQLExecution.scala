/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.util.Utils

private[sql] object SQLExecution {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  def withNewExecutionId[T](
      sqlContext: SQLContext, queryExecution: QueryExecution)(body: => T): T = {
    val sc = sqlContext.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    if (oldExecutionId == null) {
      val executionId = SQLExecution.nextExecutionId
      sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
      val r = try {
        val callSite = Utils.getCallSite()
        sqlContext.listener.onExecutionStart(
          executionId,
          callSite.shortForm,
          callSite.longForm,
          queryExecution.toString,
          SparkPlanGraph(queryExecution.executedPlan),
          System.currentTimeMillis())
        try {
          body
        } finally {
          // Ideally, we need to make sure onExecutionEnd happens after onJobStart and onJobEnd.
          // However, onJobStart and onJobEnd run in the listener thread. Because we cannot add new
          // SQL event types to SparkListener since it's a public API, we cannot guarantee that.
          //
          // SQLListener should handle the case that onExecutionEnd happens before onJobEnd.
          //
          // The worst case is onExecutionEnd may happen before onJobStart when the listener thread
          // is very busy. If so, we cannot track the jobs for the execution. It seems acceptable.
          sqlContext.listener.onExecutionEnd(executionId, System.currentTimeMillis())
        }
      } finally {
        sc.setLocalProperty(EXECUTION_ID_KEY, null)
      }
      r
    } else {
      // Don't support nested `withNewExecutionId`. This is an example of the nested
      // `withNewExecutionId`:
      //
      // class DataFrame {
      //   def foo: T = withNewExecutionId { something.createNewDataFrame().collect() }
      // }
      //
      // Note: `collect` will call withNewExecutionId
      // In this case, only the "executedPlan" for "collect" will be executed. The "executedPlan"
      // for the outer DataFrame won't be executed. So it's meaningless to create a new Execution
      // for the outer DataFrame. Even if we track it, since its "executedPlan" doesn't run,
      // all accumulator metrics will be 0. It will confuse people if we show them in Web UI.
      //
      // A real case is the `DataFrame.count` method.
      throw new IllegalArgumentException(s"$EXECUTION_ID_KEY is already set")
    }
  }

  /**
   * Captures a subset of local properties from the given Spark context that must be set for jobs
   * running as part of the same SQL query in a different thread.
   * @param sc Spark context
   * @return a map of captured local property keys to their values. The values can be null.
   */
  def captureLocalProperties(sc: SparkContext): Map[String, String] = {
    Seq(
      SQLExecution.EXECUTION_ID_KEY,
      SparkContext.SPARK_JOB_DESCRIPTION,
      SparkContext.SPARK_JOB_GROUP_ID,
      SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL,
      "spark.scheduler.pool"
    ).map { key =>
      // this can also be null, which means the local property will be unset
      key -> sc.getLocalProperty(key)
    }.toMap
  }

  /**
   * Restores captured local properties into the given Spark context. This is meant to be done in a
   * different thread than the one where the local properties were captured.
   * @param sc Spark context
   * @param capturedLocalProperties a map produced by [[captureLocalProperties]]
   */
  private[this] def setCapturedLocalProperties(
      sc: SparkContext,
      capturedLocalProperties: Map[String, String]) = {
    capturedLocalProperties.foreach { case (k, v) =>
      sc.setLocalProperty(k, v)
    }
  }

  /**
   * Runs the given code block with local properties set according to the given map constructed from
   * Spark local properties in a different thread. This is useful when running an action in a
   * different thread from the original one.
   *
   * @param sc Spark context
   * @param capturedLocalProperties local properties captured in another thread using
   *                                [[withCapturedLocalProperties]]
   * @param body a code block
   * @tparam T return type
   * @return the result of the code block
   */
  def withCapturedLocalProperties[T](
      sc: SparkContext,
      capturedLocalProperties: Map[String, String])(body: => T): T = {
    val oldCapturedLocalProperties = captureLocalProperties(sc)
    try {
      setCapturedLocalProperties(sc, capturedLocalProperties)
      body
    } finally {
      setCapturedLocalProperties(sc, oldCapturedLocalProperties)
    }
  }
}
