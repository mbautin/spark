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

package org.apache.spark.scheduler

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def ordering: Ordering[Schedulable]
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override val ordering = FIFOSchedulingAlgorithm.ordering
}

private[spark] object FIFOSchedulingAlgorithm {
  val ordering = new Ordering[Schedulable] {
    def compare(s1: Schedulable, s2: Schedulable): Int = {
      val priority1 = s1.priority
      val priority2 = s2.priority
      var res = math.signum(priority1 - priority2)
      if (res == 0) {
        val stageId1 = s1.stageId
        val stageId2 = s2.stageId
        res = math.signum(stageId1 - stageId2)
      }
      res
    }
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override val ordering = FairSchedulingAlgorithm.ordering
}

private[spark] object FairSchedulingAlgorithm {
  val ordering = new Ordering[Schedulable] {
    def compare(s1: Schedulable, s2: Schedulable): Int = {
      val minShare1 = s1.minShare
      val minShare2 = s2.minShare
      val runningTasks1 = s1.runningTasks
      val runningTasks2 = s2.runningTasks
      val s1Needy = runningTasks1 < minShare1
      val s2Needy = runningTasks2 < minShare2
      val minShareRatio1 = runningTasks1 / math.max(minShare1, 1.0)
      val minShareRatio2 = runningTasks2 / math.max(minShare2, 1.0)
      val taskToWeightRatio1 = runningTasks1 / s1.weight.toDouble
      val taskToWeightRatio2 = runningTasks2 / s2.weight.toDouble
      var comp:Int = 0

      if (s1Needy && !s2Needy) {
        -1
      } else if (!s1Needy && s2Needy) {
        1
      } else if (s1Needy && s2Needy) {
        minShareRatio1.compareTo(minShareRatio2)
      } else {
        taskToWeightRatio1.compareTo(taskToWeightRatio2)
      }
    }
  }
}

