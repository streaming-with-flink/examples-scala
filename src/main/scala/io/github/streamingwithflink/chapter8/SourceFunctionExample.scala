package io.github.streamingwithflink.chapter8

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SourceFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Long] = env.addSource(new CountSource)
    numbers.print()

    env.execute()
  }

}

class CountSource extends SourceFunction[Long] {

  var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    var cnt: Long = -1
    while (isRunning && cnt < Long.MaxValue) {
      // increment cnt
      cnt += 1
      ctx.collect(cnt)
    }

  }

  override def cancel(): Unit = isRunning = false
}

class ReplayableCountSource extends SourceFunction[Long] with CheckpointedFunction {

  var isRunning: Boolean = true
  var cnt: Long = _
  var offsetState: ListState[Long] = _

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    while (isRunning && cnt < Long.MaxValue) {
      ctx.getCheckpointLock.synchronized {
        // increment cnt
        cnt += 1
        ctx.collect(cnt)
      }
    }
  }

  override def cancel(): Unit = isRunning = false

  override def snapshotState(snapshotCtx: FunctionSnapshotContext): Unit = {
    // remove previous cnt
    offsetState.clear()
    // add current cnt
    offsetState.add(cnt)
  }

  override def initializeState(initCtx: FunctionInitializationContext): Unit = {
    // obtain operator list state to store the current cnt
    val desc = new ListStateDescriptor[Long]("offset", classOf[Long])
    offsetState = initCtx.getOperatorStateStore.getListState(desc)

    // initialize cnt variable from the checkpoint
    val it = offsetState.get()
    cnt = if (null == it || !it.iterator().hasNext) {
      -1L
    } else {
      it.iterator().next()
    }
  }
}