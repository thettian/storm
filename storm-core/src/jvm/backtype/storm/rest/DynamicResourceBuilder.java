package backtype.storm.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Constants;
import backtype.storm.coordination.BatchBoltExecutor;
import backtype.storm.coordination.CoordinatedBolt;
import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.coordination.CoordinatedBolt.IdStreamSpec;
import backtype.storm.coordination.CoordinatedBolt.SourceArgs;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.rest.JoinResult;
import backtype.storm.rest.LinearRESTInputDeclarer;
import backtype.storm.rest.PrepareRequest;
import backtype.storm.rest.RESTSpout;
import backtype.storm.rest.ReturnResults;
import backtype.storm.rest.URICheck;
import backtype.storm.topology.BaseConfigurationDeclarer;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DynamicResourceBuilder {

	String _application;
    Map<String, List<Component>> _routers = new HashMap<String, List<Component>>();
    
    
    public DynamicResourceBuilder(String application) {
        _application = application;
    }
        
    public LinearRESTInputDeclarer addBolt(String router, IBatchBolt bolt, Number parallelism) {
        return addBolt(router,new BatchBoltExecutor(bolt), parallelism);
    }
    
    public LinearRESTInputDeclarer addBolt(String router,IBatchBolt bolt) {
        return addBolt(router,bolt, 1);
    }
    
    @Deprecated
    public LinearRESTInputDeclarer addBolt(String router, IRichBolt bolt, Number parallelism) {
        if(parallelism==null) parallelism = 1; 
        Component component = new Component(bolt, parallelism.intValue());
        
        List<Component> _components = _routers.get(router);
        if(_components == null) {
        	Component _URICheckComponent = new Component(new BasicBoltExecutor(new URICheck(router)),1);
        	_components = new ArrayList<Component>();
        	_components.add(_URICheckComponent);
        }
        
        _components.add(component);
        _routers.put(router, _components);
        
        return new InputDeclarerImpl(component);
    }
    
    @Deprecated
    public LinearRESTInputDeclarer addBolt(String router,IRichBolt bolt) {
        return addBolt(router, bolt, null);
    }
    
    public LinearRESTInputDeclarer addBolt(String router, IBasicBolt bolt, Number parallelism) {
        return addBolt(router,new BasicBoltExecutor(bolt), parallelism);
    }

    public LinearRESTInputDeclarer addBolt(String router, IBasicBolt bolt) {
        return addBolt(router,bolt, null);
    }
    
    public StormTopology createRemoteTopology() {
        return createTopology(new RESTSpout(_application));
    }
    
    
    private StormTopology createTopology(RESTSpout spout) {
        final String SPOUT_ID = "spout";
        final String PREPARE_ID = "prepare-request";
        final String JOIN_ID = "join-results";
        final String RETURN_ID = "return-result";
        
        TopologyBuilder builder = new TopologyBuilder();
        
        //设置spout
        builder.setSpout(SPOUT_ID, spout);
        //PrepareRequest对所有spout进行预处理，分为三个信息流
        builder.setBolt(PREPARE_ID, new PrepareRequest()).noneGrouping(SPOUT_ID);
        
        BoltDeclarer joinDeclarer = builder.setBolt(JOIN_ID, new JoinResult(PREPARE_ID))
        						.fieldsGrouping(PREPARE_ID, PrepareRequest.RETURN_STREAM, new Fields("request"));
        
        //将Bolt以线性逻辑进行串联，并使用JOIN合并处理结果
        for(String _router:_routers.keySet()){
            handleComponent(builder, _router, PREPARE_ID, joinDeclarer);
        }
        
        //将合并后得到的结果，传送给client处理
        builder.setBolt(RETURN_ID, new ReturnResults()).noneGrouping(JOIN_ID);
        return builder.createTopology();
    }
    
    private void handleComponent(TopologyBuilder builder, String router, String PREPARE_ID, BoltDeclarer joinDeclarer){
    	List<Component> _components = _routers.get(router);
    	int i=0;
        for(; i<_components.size();i++) {
            Component component = _components.get(i);
            
            Map<String, SourceArgs> source = new HashMap<String, SourceArgs>();
            if (i==1) {
                source.put(boltId(router,i-1), SourceArgs.single());
            } else if (i>=2) {
                source.put(boltId(router,i-1), SourceArgs.all());
            }
            IdStreamSpec idSpec = null;
            if(i==_components.size()-1 && component.bolt instanceof FinishedCallback) {
                idSpec = IdStreamSpec.makeDetectSpec(PREPARE_ID, PrepareRequest.ID_STREAM);
            }
            BoltDeclarer declarer = builder.setBolt(
                    boltId(router,i),
                    new CoordinatedBolt(component.bolt, source, idSpec),
                    component.parallelism);
            
            for(Map conf: component.componentConfs) {
                declarer.addConfigurations(conf);
            }
            
            if(idSpec!=null) {
                declarer.fieldsGrouping(idSpec.getGlobalStreamId().get_componentId(), PrepareRequest.ID_STREAM, new Fields("request"));
            }
            if(i==0 && component.declarations.isEmpty()) {
                declarer.noneGrouping(PREPARE_ID, PrepareRequest.ARGS_STREAM);
            } else {
                String prevId;
                if(i==0) {
                    prevId = PREPARE_ID;
                } else {
                    prevId = boltId(router,i-1);
                }
                for(InputDeclaration declaration: component.declarations) {
                    declaration.declare(prevId, declarer);
                }
            }
            if(i>0) {
                declarer.directGrouping(boltId(router,i-1), Constants.COORDINATED_STREAM_ID); 
            }
        }
        
        //获取最后一个处理Bolt
        IRichBolt lastBolt = _components.get(_components.size()-1).bolt;
        OutputFieldsGetter getter = new OutputFieldsGetter();
        lastBolt.declareOutputFields(getter);
        //分析最后一个处理Bolt所在的stream
        Map<String, StreamInfo> streams = getter.getFieldsDeclaration();
        if(streams.size()!=1) {
            throw new RuntimeException("Must declare exactly one stream from last bolt in LinearRESTTopology");
        }
        //分析最后一个处理Bolt是否存放返回结果
        String outputStream = streams.keySet().iterator().next();
        List<String> fields = streams.get(outputStream).get_output_fields();
        if(fields.size()!=2) {
            throw new RuntimeException("Output stream of last component in LinearRESTTopology must contain exactly two fields. The first should be the request id, and the second should be the result.");
        }
        //最后一个处理Bolt的结果传送给joinResult进行合并
        joinDeclarer.fieldsGrouping(boltId(router,i-1), outputStream, new Fields(fields.get(0)));
                
    }
    
    private String boltId(String key,int index) {
        return key + index;
    }
    
    private static class Component {
        public IRichBolt bolt;
        public int parallelism;
        public List<Map> componentConfs;
        public List<InputDeclaration> declarations = new ArrayList<InputDeclaration>();
        
        public Component(IRichBolt bolt, int parallelism) {
            this.bolt = bolt;
            this.parallelism = parallelism;
            this.componentConfs = new ArrayList();
        }
    }
    
    private static interface InputDeclaration {
        public void declare(String prevComponent, InputDeclarer declarer);
    }
    
    private class InputDeclarerImpl extends BaseConfigurationDeclarer<LinearRESTInputDeclarer> implements LinearRESTInputDeclarer {
        Component _component;
        
        public InputDeclarerImpl(Component component) {
            _component = component;
        }
        
        @Override
        public LinearRESTInputDeclarer fieldsGrouping(final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer fieldsGrouping(final String streamId, final Fields fields) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.fieldsGrouping(prevComponent, streamId, fields);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer globalGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer globalGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.globalGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer shuffleGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer shuffleGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.shuffleGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer localOrShuffleGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer localOrShuffleGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.localOrShuffleGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }
        
        @Override
        public LinearRESTInputDeclarer noneGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer noneGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.noneGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer allGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer allGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.allGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer directGrouping() {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer directGrouping(final String streamId) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.directGrouping(prevComponent, streamId);
                }                
            });
            return this;
        }
        
        @Override
        public LinearRESTInputDeclarer customGrouping(final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.customGrouping(prevComponent, grouping);
                }                
            });
            return this;
        }

        @Override
        public LinearRESTInputDeclarer customGrouping(final String streamId, final CustomStreamGrouping grouping) {
            addDeclaration(new InputDeclaration() {
                @Override
                public void declare(String prevComponent, InputDeclarer declarer) {
                    declarer.customGrouping(prevComponent, streamId, grouping);
                }                
            });
            return this;
        }
        
        private void addDeclaration(InputDeclaration declaration) {
            _component.declarations.add(declaration);
        }

        @Override
        public LinearRESTInputDeclarer addConfigurations(Map conf) {
            _component.componentConfs.add(conf);
            return this;
        }
    }
}
