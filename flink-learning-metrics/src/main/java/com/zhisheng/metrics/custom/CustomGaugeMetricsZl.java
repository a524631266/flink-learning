package com.zhisheng.metrics.custom;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class CustomGaugeMetricsZl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = e.getConfig();
        config.setParallelism(1);
        long executionRetryDelay = config.getExecutionRetryDelay();
        print(executionRetryDelay, "executionRetryDelay");
        long autoWatermarkInterval = config.getAutoWatermarkInterval();
        print(autoWatermarkInterval, "autoWatermarkInterval");
        print(config.getLatencyTrackingInterval(), "LatencyTrackingInterval");
        print(config.getTaskCancellationInterval(), "TaskCancellationInterval");

        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        DataStreamSource<Integer> integerDataStreamSource = e.fromElements(1, 2, 3, 4);

        SingleOutputStreamOperator<Integer> singleO = e.addSource(new Source()).map(v -> v);
        // 两条流
        singleO.countWindowAll(4).sum(0).print("sumSource");
        singleO.print("mapSource");
        integerDataStreamSource.map(new Mapper()).print("fromElements");

        e.fromCollection(Arrays.asList(1,2,3,5,4)).print("fromCollection");

        e.execute();

        System.in.read();
    }

    private static void print(Object value, String tag) {
        System.out.println(String.format("%s: %s",  tag, value));
    }

    private static class Mapper implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) throws Exception {
            return value *2;
        }
    }

    private static class Source  extends RichSourceFunction<Integer> {
        private volatile boolean isOpen = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (isOpen) {
                ctx.collect(new Random().nextInt(100));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isOpen = false;
        }
    }
}
