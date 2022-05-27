package cn.shellbin.flink.datastream;

import cn.shellbin.flink.bean.WaterSensor;
import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: yangxianbin
 * @Date: 2022/5/26 11:12
 * <p>
 * 每隔5秒钟统计过去5秒钟内每个探针值的sum，允许乱序时间2秒,窗口等待增量计算3秒
 * 0、1.12后watermark默认为eventTime
 * 1、watermark = eventTime - 2s
 * 2、按照探针id分组
 * 3、tumbling窗口5秒，事件根据eventTime被分配到不同的窗口
 * 4、watermark大于窗口endTime时触发窗口计算
 * 数据来源: sensor.txt
 * nc -l 1234
 * 结果：
 * 1577844002增量计算；1577844006迟到数据旁路输出
 * <p>
 */
public class WindowDemo1 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 窗口关闭后的延迟数据放到侧输出流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("windowLateData") {
        };

        //0、从文件中获取数据
        SingleOutputStreamOperator<WaterSensor> streamSource = env.socketTextStream("localhost", 1234)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //1、生成watermark，乱序时间为2秒
        SingleOutputStreamOperator<WaterSensor> watermarkedSource = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000));

        //2、按照探针id分组
        KeyedStream<WaterSensor, String> keyedStream = watermarkedSource.keyBy(WaterSensor::getId);

        //3、5秒翻转窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //窗口会对延迟的数据进行增量计算
        windowedStream.allowedLateness(Time.seconds(3));
        windowedStream.sideOutputLateData(outputTag);

        //4、窗口计算
        SingleOutputStreamOperator<WindowOutput> reduce = windowedStream.apply(new WindowFunction<WaterSensor, WindowOutput, String, TimeWindow>() {
            @Override
            public void apply(String sensorId, TimeWindow window, Iterable<WaterSensor> input, Collector<WindowOutput> out) throws Exception {
                int sum = 0;
                List<WaterSensor> sensorList = new ArrayList<>();
                for (WaterSensor waterSensor : input) {
                    sum += waterSensor.getVc();
                    sensorList.add(waterSensor);
                }
                out.collect(new WindowOutput(new WaterSensor(sensorId, window.getStart(), sum), sensorList));
            }
        });
        reduce.print();
        DataStream<WaterSensor> sideOutput = reduce.getSideOutput(outputTag);
        sideOutput.print("sideOutput");
        env.execute("watermark and window");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class WindowOutput {
        private WaterSensor waterSensor;
        private List<WaterSensor> sensorList;

        @Override
        public String toString() {
            return "WindowOutput: waterSensor=" + waterSensor + "=====" +
                    "sensorList" + Joiner.on(";").join(sensorList);
        }
    }

}
