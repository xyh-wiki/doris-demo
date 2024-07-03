package wiki.xyh.dao;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import wiki.xyh.config.JobConfig;

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2024/6/26 21:16
 * @Description: TODO
 */
public class KafkaReader {
    public static DataStream<String> readDataFromKafka(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        Properties kafkaProperties = JobConfig.configureKafka();
        return env.fromSource(
                JobConfig.createKafkaSource(kafkaProperties, parameterTool),
                WatermarkStrategy.noWatermarks(),
                "kafka 读取数据"
        ).setParallelism(parameterTool.getInt("kafka.source.parallelism", 1));
    }
}
