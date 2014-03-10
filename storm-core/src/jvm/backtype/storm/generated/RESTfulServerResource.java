package backtype.storm.generated;

import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.restlet.service.MetadataService;

import backtype.storm.generated.RESTful.RESTfulProcessor;

/**
 * The Restlet ServerResource for storm REST.
 * @author thettian
 *
 */
public class RESTfulServerResource extends ServerResource{
	private RESTfulProcessor processor;
	private MetadataService metadataService;
	
	/**
	 * Overrid the {@doInit} to get the request processor.
	 */
	@Override
	public void doInit(){
		processor = (RESTfulProcessor) getContext().getAttributes().get("processor");
		metadataService = new MetadataService();
	}
	
	/**
     * Handle the HTTP GET method by returning a simple textual representation.
     */
    @Override
    protected Representation get() throws ResourceException {
        String _result = processor.execute(getRequest().getResourceRef().getPath());
        
        return handleStringResult(_result);
    }
    
    /**
     * Handle the HTTP POST method by returning a simple textual representation.
     */
    protected Representation post() throws ResourceException{
    	String _result = processor.execute(getRequest().getResourceRef().getPath());
    	
    	return handleStringResult(_result);
    }

    /**
     * Handle the HTTP OPTIONS method by illustrating the impact of throwing an
     * exception.
     */
    @Override
    protected Representation options() throws ResourceException {
        System.out.println("The OPTIONS method of root resource was invoked.");
        throw new RuntimeException("Not yet implemented");
    }
    
    private StringRepresentation handleStringResult(String text){
    	
    	if(text == null) text = "";
    	StringRepresentation _result = new StringRepresentation(text);
    	
    	int _index = text.indexOf(':');
    	if(_index > 0){
    		String _extention = text.substring(0, _index - 1);
    		
    		_result.setText(text.substring(_index + 1));
    		MediaType _mediatype = metadataService.getMediaType(_extention);
    		if(_mediatype != null)
        		_result.setMediaType(_mediatype);
    	}
    	
    	return _result;
    }
}
