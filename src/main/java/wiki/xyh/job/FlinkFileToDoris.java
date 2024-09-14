package wiki.xyh.job;

import com.alibaba.fastjson2.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import wiki.xyh.config.JobConfig;
import wiki.xyh.process.JsonSourceMapper;
import wiki.xyh.process.NonNullFilter;


import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2024/6/26 20:41
 * @Description: Flink 读取多个文件，提取 _source 并添加字段，写入 Doris
 */
public class FlinkFileToDoris {

    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        env.getConfig().setGlobalJobParameters(parameterTool);

//        JobConfig.configureFlinkEnvironment(env, parameterTool);
        env.getCheckpointConfig().disableCheckpointing();

        // 配置 DorisSink
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
        executionBuilder.setLabelPrefix("label-flink-doris")
                .setDeletable(false)
                .setStreamLoadProp(properties);

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisBuilder.build())
                .setSerializer(new SimpleStringSerializer());

        // 读取多个文件内容，支持多个文件或目录，使用逗号分隔
        String inputPaths = parameterTool.get("input.file.paths");

        TextInputFormat format = new TextInputFormat(new Path(inputPaths.trim()));  // 去除多余空格
        DataStream<String> fileStream = env.readFile(format, inputPaths.trim(), FileProcessingMode.PROCESS_ONCE, 100).setParallelism(parameterTool.getInt("split.parallelism", 1));


        // 拆开 map 和 filter 操作，并不使用 lambda 表达式
        SingleOutputStreamOperator<String> mappedStream = fileStream
                .map(new JsonSourceMapper())  // 使用自定义的 MapFunction
                .setParallelism(parameterTool.getInt("map.parallelism", 1));  // 设置并行度

// filter 操作
        SingleOutputStreamOperator<String> resultStream = mappedStream
                .filter(new NonNullFilter())  // 使用自定义的 FilterFunction
                .setParallelism(parameterTool.getInt("filter.parallelism", 1));  // 设置并行度

        // 将结果写入 Doris
        resultStream.sinkTo(builder.build()).setParallelism(parameterTool.getInt("doris.sink.parallelism", 1));

        // 执行 Flink 作业
        env.execute("Flink File to Doris");
    }
}
