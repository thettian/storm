package backtype.storm.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StaticResource extends BaseBasicBolt {
	private String BASE_PATH = "webapp";
	
	public StaticResource(){}
	public StaticResource(String base_path){
		this.BASE_PATH = base_path;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Map _request = (Map) JSONValue.parse(tuple.getString(1));
		String uri = (String) _request.get("URI");
		String _result = findAndReadFile(uri);
		collector.emit(new Values(tuple.getValue(0), _result));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

	private String findAndReadFile(String name) {
		String _result = "";

		int _index = name.lastIndexOf('.');
		_result += name.substring(_index + 1) + ":";

		List<URL> resources = Utils.findResources(this.BASE_PATH
				+ name);
		if (resources.size() == 0) {
			_result += "error-404: " + name + " File not Found!";
		} else if (resources.size() > 1) {
			_result += "error-505: " + name + " Multiple file!";
		} else {
			URL resource = resources.get(0);
			String s = null;

			try {
				BufferedReader _reader = new BufferedReader(
						new InputStreamReader(resource.openStream()));
				while ((s = _reader.readLine()) != null) {
					_result += s;
				}
			} catch (IOException e) {
				_result += "error-505: File Read Error!";
			}

		}

		return _result;
	}

}
