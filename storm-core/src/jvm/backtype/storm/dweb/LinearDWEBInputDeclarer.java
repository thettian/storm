package backtype.storm.dweb;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.ComponentConfigurationDeclarer;
import backtype.storm.tuple.Fields;

public interface LinearDWEBInputDeclarer extends ComponentConfigurationDeclarer<LinearDWEBInputDeclarer>{
	public LinearDWEBInputDeclarer fieldsGrouping(Fields fields);
    public LinearDWEBInputDeclarer fieldsGrouping(String streamId, Fields fields);

    public LinearDWEBInputDeclarer globalGrouping();
    public LinearDWEBInputDeclarer globalGrouping(String streamId);

    public LinearDWEBInputDeclarer shuffleGrouping();
    public LinearDWEBInputDeclarer shuffleGrouping(String streamId);

    public LinearDWEBInputDeclarer localOrShuffleGrouping();
    public LinearDWEBInputDeclarer localOrShuffleGrouping(String streamId);
    
    public LinearDWEBInputDeclarer noneGrouping();
    public LinearDWEBInputDeclarer noneGrouping(String streamId);

    public LinearDWEBInputDeclarer allGrouping();
    public LinearDWEBInputDeclarer allGrouping(String streamId);

    public LinearDWEBInputDeclarer directGrouping();
    public LinearDWEBInputDeclarer directGrouping(String streamId);
    
    public LinearDWEBInputDeclarer customGrouping(CustomStreamGrouping grouping);
    public LinearDWEBInputDeclarer customGrouping(String streamId, CustomStreamGrouping grouping);
}
