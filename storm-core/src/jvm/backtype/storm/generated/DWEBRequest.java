package backtype.storm.generated;

import java.util.Map;

import org.json.simple.JSONValue;

public class DWEBRequest {
	private String url;
	private Map<String, String> attributes;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}
	
	@Override
	public String toString(){
		this.attributes.put("URI", url);
		return JSONValue.toJSONString(this.attributes);
	}

}
