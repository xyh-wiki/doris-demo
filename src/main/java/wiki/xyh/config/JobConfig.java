package wiki.xyh.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2024/6/26 21:14
 * @Description: TODO
 */
public class JobConfig {
    // 设置 Flink 运行环境
    public static void configureFlinkEnvironment(StreamExecutionEnvironment env, ParameterTool parameterTool) {
//        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(5 * 60 * 1000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointTimeout(6 * 1000 * 1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10L)));
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(parameterTool.getRequired("checkpoint.dir")));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    // Kafka 配置
    public static Properties configureKafka() {
        Properties properties = new Properties();
        properties.setProperty("request.timeout.ms", "214748364");
        properties.setProperty("metadata.fetch.timeout.ms", "214748364");
        properties.setProperty("max.poll.records", "5000");
        properties.setProperty("retries", "20");
        properties.setProperty("linger.ms", "300");
        properties.setProperty("log.level", "warn");
        return properties;
    }

    // 创建 Kafka Source
    public static KafkaSource<String> createKafkaSource(Properties properties, ParameterTool parameterTool) {
        return  KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get("kafka.server"))
                .setTopics(parameterTool.get("kafka.topic"))
                .setGroupId(parameterTool.get("kafka.groupId"))
                .setProperties(properties)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static void setHadoopUsername(ParameterTool parameterTool) {
        System.setProperty("HADOOP_USER_NAME", parameterTool.get("hadoop.user.name", "root"));
    }
}
