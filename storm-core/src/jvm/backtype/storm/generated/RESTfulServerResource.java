package backtype.storm.generated;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

import backtype.storm.generated.RESTful.RESTfulProcessor;

/**
 * The Restlet ServerResource for storm REST.
 * @author thettian
 *
 */
public class RESTfulServerResource extends ServerResource{
	private RESTfulProcessor processor;
	
	/**
	 * Overrid the {@doInit} to get the request processor.
	 */
	@Override
	public void doInit(){
		processor = (RESTfulProcessor) getContext().getAttributes().get("processor");
	}
	
	/**
     * Handle the HTTP GET method by returning a simple textual representation.
     */
    @Override
    protected Representation get() throws ResourceException {
        String result = processor.execute(getRequest().getResourceRef().getPath());
        return new StringRepresentation(result);
    }
    
    /**
     * Handle the HTTP POST method by returning a simple textual representation.
     */
    protected Representation post() throws ResourceException{
    	String result = processor.execute(getRequest().getResourceRef().getPath());
    	return new StringRepresentation(result);
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
}
