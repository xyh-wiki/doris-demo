package wiki.xyh.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @Author: XYH
 * @Date: 2021/12/2 11:54 上午
 * @Description: 不含文书的bean
 */

/**
 * schema总共对应11个类型分别是
 * 管辖案件 01
 * 刑事案件 02
 * 民事案件 03
 * 行政案件 04
 * 国家赔偿与司法救助案件 05
 * 区际司法协助案件 06
 * 国际司法协助案件 07
 * 司法制裁案件 08
 * 非诉保全审查案件 09
 * 执行类案件 10
 * 强制清算与破产案件 11
 */

@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WsBean {


    private String c_baah_source;           //本案案号(公开,内网)
    private String c_ay_source;             //案由(公开,内网)
    private String c_jbfymc_source;         //经办法院名称(公开,内网)
    private String rowkey_source;           //rowkey(公开,内网)
    private String c_md5_source;            //文书 md5编码(公开,内网)
    private String c_bt_source;             //文书标题(公开)
    private String c_wslx_source;           //文书类型(公开)
    private String c_fycj_source;           //法院层级(公开)
    private String c_cprq_source;           //裁判日期(公开)
    private String c_sf_source;             //省份名称(公开)
    private String c_ajlxmc_source;         //案件类型名称(公开)
    private String c_spcx_source;           //审判程序(公开)
    private String c_crc_source;            //文书 crc(内网)
    private String original_dt_source;      //内网文书ods时间(内网)
    private String d_sarq_source;           //案件收案日期(内网)
    private String c_stm_source;            //案件实体码(内网)
    private String c_mc_source;             //文书文件命名(内网)
    private String c_nr_source;             //文书下载URL(内网)
    private String n_jbfy_source;           //经办法院ID(内网)
    private String n_ajzlx_source;          //案件子类型编码(内网)
    private String fbId_source;             //法标ID(内网)
    private String c_sfid_source;            //省份id(内网)
    private String n_lb_source;             //类别(内网)
    private String sensitive_word;          //敏感词
    private int data_state;                 //数据状态
}
