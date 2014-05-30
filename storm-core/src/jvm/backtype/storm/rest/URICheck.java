package backtype.storm.rest;

import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class URICheck extends BaseBasicBolt{
	private String router;
	
	public URICheck(String router){
		this.router = router;
	}

    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	Map _request = (Map) JSONValue.parse(tuple.getString(1));
		String uri = (String) _request.get("URI");
		if(uri.equals(router)){
			_request.remove("URI");
			collector.emit(new Values(tuple.getValue(0), JSONValue.toJSONString(_request)));
		}
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "result"));
    }
    
}
