package wiki.xyh.bean;
/**
 * @ClassName: KafkaSourceBean
 * @Time: 2024/8/6 15:31
 * @Author: XYH
 * @Description: TODO
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @Author: XYH
 * @Date: 2024/8/6 15:31
 * @Description: TODO
 */
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaSourceBean {
    String data;
    WsBean wsBean;
}
