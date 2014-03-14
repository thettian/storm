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
import backtype.storm.rest.JoinResult;
import backtype.storm.rest.LinearRESTInputDeclarer;
import backtype.storm.rest.PrepareRequest;
import backtype.storm.rest.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BaseConfigurationDeclarer;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class LinearRESTTopologyBuilder {
	String _application;
    List<Component> _components = new ArrayList<Component>();
    
    
    public LinearRESTTopologyBuilder(String application) {
        _application = application;
    }
        
    public LinearRESTInputDeclarer addBolt(IBatchBolt bolt, Number parallelism) {
        return addBolt(new BatchBoltExecutor(bolt), parallelism);
    }
    
    public LinearRESTInputDeclarer addBolt(IBatchBolt bolt) {
        return addBolt(bolt, 1);
    }
    
    @Deprecated
    public LinearRESTInputDeclarer addBolt(IRichBolt bolt, Number parallelism) {
        if(parallelism==null) parallelism = 1; 
        Component component = new Component(bolt, parallelism.intValue());
        _components.add(component);
        return new InputDeclarerImpl(component);
    }
    
    @Deprecated
    public LinearRESTInputDeclarer addBolt(IRichBolt bolt) {
        return addBolt(bolt, null);
    }
    
    public LinearRESTInputDeclarer addBolt(IBasicBolt bolt, Number parallelism) {
        return addBolt(new BasicBoltExecutor(bolt), parallelism);
    }

    public LinearRESTInputDeclarer addBolt(IBasicBolt bolt) {
        return addBolt(bolt, null);
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
        builder.setSpout(SPOUT_ID, spout);
        builder.setBolt(PREPARE_ID, new PrepareRequest())
                .noneGrouping(SPOUT_ID);
        int i=0;
        for(; i<_components.size();i++) {
            Component component = _components.get(i);
            
            Map<String, SourceArgs> source = new HashMap<String, SourceArgs>();
            if (i==1) {
                source.put(boltId(i-1), SourceArgs.single());
            } else if (i>=2) {
                source.put(boltId(i-1), SourceArgs.all());
            }
            IdStreamSpec idSpec = null;
            if(i==_components.size()-1 && component.bolt instanceof FinishedCallback) {
                idSpec = IdStreamSpec.makeDetectSpec(PREPARE_ID, PrepareRequest.ID_STREAM);
            }
            BoltDeclarer declarer = builder.setBolt(
                    boltId(i),
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
                    prevId = boltId(i-1);
                }
                for(InputDeclaration declaration: component.declarations) {
                    declaration.declare(prevId, declarer);
                }
            }
            if(i>0) {
                declarer.directGrouping(boltId(i-1), Constants.COORDINATED_STREAM_ID); 
            }
        }
        
        IRichBolt lastBolt = _components.get(_components.size()-1).bolt;
        OutputFieldsGetter getter = new OutputFieldsGetter();
        lastBolt.declareOutputFields(getter);
        Map<String, StreamInfo> streams = getter.getFieldsDeclaration();
        if(streams.size()!=1) {
            throw new RuntimeException("Must declare exactly one stream from last bolt in LinearRESTTopology");
        }
        String outputStream = streams.keySet().iterator().next();
        List<String> fields = streams.get(outputStream).get_output_fields();
        if(fields.size()!=2) {
            throw new RuntimeException("Output stream of last component in LinearRESTTopology must contain exactly two fields. The first should be the request id, and the second should be the result.");
        }

        builder.setBolt(JOIN_ID, new JoinResult(PREPARE_ID))
                .fieldsGrouping(boltId(i-1), outputStream, new Fields(fields.get(0)))
                .fieldsGrouping(PREPARE_ID, PrepareRequest.RETURN_STREAM, new Fields("request"));
        //i++;
        builder.setBolt(RETURN_ID, new ReturnResults())
                .noneGrouping(JOIN_ID);
        return builder.createTopology();
    }
    
    private static String boltId(int index) {
        return "bolt" + index;
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
