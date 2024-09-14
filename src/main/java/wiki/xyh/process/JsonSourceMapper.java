package wiki.xyh.process;

import org.apache.flink.api.common.functions.MapFunction;
import com.alibaba.fastjson2.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JsonSourceMapper implements MapFunction<String, String> {

    @Override
    public String map(String value) {
        // 解析 JSON 对象
        JSONObject jsonObject = JSONObject.parseObject(value);
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
}
