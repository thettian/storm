package backtype.storm.rest;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.ComponentConfigurationDeclarer;
import backtype.storm.tuple.Fields;

public interface LinearRESTInputDeclarer extends ComponentConfigurationDeclarer<LinearRESTInputDeclarer>{
	public LinearRESTInputDeclarer fieldsGrouping(Fields fields);
    public LinearRESTInputDeclarer fieldsGrouping(String streamId, Fields fields);

    public LinearRESTInputDeclarer globalGrouping();
    public LinearRESTInputDeclarer globalGrouping(String streamId);

    public LinearRESTInputDeclarer shuffleGrouping();
    public LinearRESTInputDeclarer shuffleGrouping(String streamId);

    public LinearRESTInputDeclarer localOrShuffleGrouping();
    public LinearRESTInputDeclarer localOrShuffleGrouping(String streamId);
    
    public LinearRESTInputDeclarer noneGrouping();
    public LinearRESTInputDeclarer noneGrouping(String streamId);

    public LinearRESTInputDeclarer allGrouping();
    public LinearRESTInputDeclarer allGrouping(String streamId);

    public LinearRESTInputDeclarer directGrouping();
    public LinearRESTInputDeclarer directGrouping(String streamId);
    
    public LinearRESTInputDeclarer customGrouping(CustomStreamGrouping grouping);
    public LinearRESTInputDeclarer customGrouping(String streamId, CustomStreamGrouping grouping);
}
