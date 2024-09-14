package wiki.xyh.job;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

/**
 * @Author: XYH
 * @Date: 2024/6/26
 * @Description: 逐行读取大JSON文件，并通过Doris Stream Load写入Doris表
 */
public class JsonToDoris {

    private static String DORIS_STREAM_LOAD_URL;
    private static String DORIS_USER;
    private static String DORIS_PASSWORD;
    private static List<String> filePaths;
    private static final int BATCH_SIZE = 10000;  // 每批处理的行数

    public static void main(String[] args) throws IOException {

        // 加载配置文件
        Properties properties = loadProperties(args[0]);

        // 从配置文件中读取参数
        DORIS_STREAM_LOAD_URL = properties.getProperty("doris.stream.load.url");
        DORIS_USER = properties.getProperty("doris.user");
        DORIS_PASSWORD = properties.getProperty("doris.password");

        // 获取文件路径列表 (多个文件路径用逗号分隔)
        String filePathConfig = properties.getProperty("file.paths");
        filePaths = Arrays.asList(filePathConfig.split(","));

        // 处理每个 JSON 文件
        for (String filePath : filePaths) {
            System.out.println("Processing file: " + filePath.trim());
            processJsonFileInBatches(filePath.trim());
        }

        System.out.println("All files processed successfully.");
    }

    /**
     * 读取配置文件
     *
     * @param configFilePath 配置文件路径
     * @return Properties 对象
     * @throws IOException
     */
    private static Properties loadProperties(String configFilePath) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(configFilePath))) {
            properties.load(input);
        }
        return properties;
    }

    /**
     * 逐行读取 JSON 文件并批量处理，避免内存溢出
     *
     * @param filePath 文件路径
     * @throws IOException
     */
    private static void processJsonFileInBatches(String filePath) throws IOException {
        List<String> batch = new ArrayList<>(BATCH_SIZE);

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 将每一行数据添加到批处理集合
                batch.add(line);

                // 当批处理达到 BATCH_SIZE 时，发送到 Doris
                if (batch.size() == BATCH_SIZE) {
                    writeBatchToDoris(batch);
                    batch.clear();  // 清空批处理集合
                }
            }
        }

        // 处理最后剩余的行（不足一个批次）
        if (!batch.isEmpty()) {
            writeBatchToDoris(batch);
        }
    }

    /**
     * 将一批 JSON 数据写入 Doris
     *
     * @param batch JSON 数据的批处理集合
     */
    private static void writeBatchToDoris(List<String> batch) {
        // 将批处理集合中的 JSON 数据拼接成一个字符串
        StringBuilder jsonData = new StringBuilder();
        for (String line : batch) {
            // 处理每一行的 JSON 数据
            String processedJson = processJsonData(line);
            if (processedJson != null) {
                jsonData.append(processedJson).append("\n");
            }
        }

        // 将处理好的数据写入 Doris
        writeToDoris(jsonData.toString());
    }

    /**
     * 处理 JSON 数据，提取 _source 字段，或者添加额外的字段
     *
     * @param jsonData 原始 JSON 数据
     * @return 处理后的 JSON 数据
     */
    private static String processJsonData(String jsonData) {
        // 使用 fastjson 解析并处理 JSON 数据
        JSONObject jsonObject = JSON.parseObject(jsonData);
        JSONObject source = jsonObject.getJSONObject("_source");

        if (source != null) {
            // 获取当前时间并格式化
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String currentDateTime = dateTimeFormat.format(new Date());
            String currentDate = dateFormat.format(new Date());

            // 添加时间戳字段
            source.put("c_md5_source", source.getString("c_md5"));
            source.put("create_time", currentDateTime);
            source.put("update_time", currentDateTime);
            source.put("c_dt", currentDate);
        }

        return source != null ? source.toJSONString() : null;
    }
    /**
     * 将处理后的 JSON 数据写入 Doris
     *
     * @param jsonData 处理后的 JSON 数据
     */
    private static void writeToDoris(String jsonData) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            // 使用 PUT 请求
            HttpPut put = new HttpPut(DORIS_STREAM_LOAD_URL);

            // 设置基本认证
            put.setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((DORIS_USER + ":" + DORIS_PASSWORD).getBytes()));

            // 设置自定义配置项：format=json 和 read_json_by_line=true
            put.setHeader("format", "json");  // 指定数据格式为 JSON
            put.setHeader("read_json_by_line", "true");  // 按行读取 JSON 数据

            // 设置 Expect: 100-continue 头
            put.setHeader("Expect", "100-continue");

            // 设置请求实体为 JSON 数据
            StringEntity entity = new StringEntity(jsonData, StandardCharsets.UTF_8);
            entity.setContentType("application/json");
            put.setEntity(entity);
            put.setHeader("Content-Type", "application/json");

            // 发送请求并处理响应
            HttpResponse response = httpClient.execute(put);
            HttpEntity responseEntity = response.getEntity();

            int statusCode = response.getStatusLine().getStatusCode();
            System.out.println("HTTP Status Code: " + statusCode);

            if (responseEntity != null) {
                try (InputStream instream = responseEntity.getContent()) {

                    String result = new BufferedReader(new InputStreamReader(instream, StandardCharsets.UTF_8)).lines().collect(java.util.stream.Collectors.joining("\n"));
                    System.out.println("Doris response: " + result);

                    // 检查 Doris 的响应，确认数据是否成功加载
                    if (result.contains("\"Status\":\"Success\"") || result.contains("OK")) {
                        System.out.println("Data successfully written to Doris.");
                    } else {
                        System.err.println("Failed to write data to Doris: " + result);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error writing to Doris: " + e.getMessage());
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
