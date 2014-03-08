package backtype.storm.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.ServiceRegistry;
import backtype.storm.utils.Utils;

public class RESTSpout extends BaseRichSpout{
	public static Logger LOG = LoggerFactory.getLogger(RESTSpout.class);
	public static final String _function = "REST";
	
	SpoutOutputCollector _collector;
	List<DRPCInvocationsClient> _clients = new ArrayList<DRPCInvocationsClient>();
	String _application;
	String _local_rest_id = null;
	
	private static class DRPCMessageId {
        String id;
        int index;
        
        public DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
        }
    }
	
	public RESTSpout(String application){
		_application = application;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		if(_local_rest_id == null){
			int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            int index = context.getThisTaskIndex();
            
            int port = Utils.getInt(conf.get(Config.REST_INVOCATIONS_PORT));
            List<String> servers = (List<String>) conf.get(Config.REST_SERVERS);
            if(servers == null || servers.isEmpty()){
            	throw new RuntimeException("No REST configured for topology");
            }
            if(numTasks < servers.size()){
            	for(String s: servers) {
            		_clients.add(new DRPCInvocationsClient(s,port));
            	}
            }else{
            	int i = index % servers.size();
            	_clients.add(new DRPCInvocationsClient(servers.get(i),port));
            }
		}
		
	}
	
	@Override
	public void close(){
		for(DRPCInvocationsClient client: _clients){
			client.close();
		}
	}

	public void nextTuple() {
		boolean gotRequest = false;
        if(_local_rest_id==null) {
            for(int i=0; i<_clients.size(); i++) {
                DRPCInvocationsClient client = _clients.get(i);
                try {
                    DRPCRequest req = client.fetchRequest(RESTSpout._function);
                    if(req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", client.getHost());
                        returnInfo.put("port", client.getPort());
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), i));
                        break;
                    }
                } catch (TException e) {
                    LOG.error("Failed to fetch REST result from DRPC server", e);
                }
            }
        } else {
            DistributedRPCInvocations.Iface drpc = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_rest_id);
            if(drpc!=null) { // can happen during shutdown of rest while topology is still up
                try {
                    DRPCRequest req = drpc.fetchRequest(RESTSpout._function);
                    if(req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", _local_rest_id);
                        returnInfo.put("port", 0);
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), 0));
                    }
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if(!gotRequest) {
            Utils.sleep(1);
        }
		
	}
	
	@Override
	public void ack(Object msgId){
		
	}
	
	@Override
	public void fail(Object msgId){
		DRPCMessageId did = (DRPCMessageId) msgId;
        DistributedRPCInvocations.Iface client;
        
        if(_local_rest_id == null) {
            client = _clients.get(did.index);
        } else {
            client = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_rest_id);
        }
        try {
            client.failRequest(did.id);
        } catch (TException e) {
            LOG.error("Failed to fail request", e);
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("args","return-info"));
		
	}

}
