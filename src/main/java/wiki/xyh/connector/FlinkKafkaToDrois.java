package wiki.xyh.connector;

import com.alibaba.fastjson2.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import wiki.xyh.bean.KafkaSourceBean;
import wiki.xyh.bean.WsBean;
import wiki.xyh.config.JobConfig;
import wiki.xyh.dao.KafkaReader;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2024/6/26 20:41
 * @Description: flink 写入 doris， source 是 kafka， 数据为 json 格式
 */
public class FlinkKafkaToDrois {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        env.getConfig().setGlobalJobParameters(parameterTool);

        JobConfig.configureFlinkEnvironment(env, parameterTool);

        DorisSink.Builder<String> builder = DorisSink.builder();

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();

        dorisBuilder.setFenodes(parameterTool.get("feIpAndPort"))
                .setTableIdentifier(parameterTool.get("doris.sink.table"))
                .setUsername(parameterTool.get("doris.username"))
                .setPassword(parameterTool.get("doris.password"));

        Properties properties = new Properties();

        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();

        executionBuilder.setLabelPrefix("label-doris-2")
                .setDeletable(false)
                .setStreamLoadProp(properties);

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisBuilder.build())
                .setSerializer(new SimpleStringSerializer());

        DataStream<String> kafkaSource = KafkaReader.readDataFromKafka(env, parameterTool);

        SingleOutputStreamOperator<String> resultStream = kafkaSource.map((MapFunction<String, String>) value -> {
            KafkaSourceBean kafkaSourceBean = JSONObject.parseObject(value, KafkaSourceBean.class);

            String data = kafkaSourceBean.getData();
            JSONObject wsBeanObject = JSONObject.from(kafkaSourceBean.getWsBean());
            JSONObject parsingObject = JSONObject.parseObject(data);

            for (String key : wsBeanObject.keySet()) {
                parsingObject.put(key, wsBeanObject.get(key));
            }

            return JSONObject.toJSONString(parsingObject);
        });

        resultStream.print();

        // 执行 Flink 作业
        env.execute("Flink Kafka to Doris");

    }
}
