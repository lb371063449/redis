package com.roncoo.eshop.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 日志解析的bolt
 *
 * @author Rinbo
 */
@Slf4j
public class LogParseBolt extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String message = tuple.getStringByField("message");

        log.debug("【LogParseBolt接收到一条日志】message= {}", message);

        JSONObject messageJSON = JSONObject.parseObject(message);
        JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args");
        Long productId = uriArgsJSON.getLong("productId");

        if (productId != null) {
            collector.emit(new Values(productId));
            log.debug("【LogParseBolt发射出去一个商品id】productId= {}", productId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }

}
