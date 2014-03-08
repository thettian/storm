package backtype.storm.generated;

import org.restlet.Context;
import org.restlet.Server;
import org.restlet.data.Protocol;

/**
 * The components for storm REST.
 * @author thettian
 *
 */
public class RESTful {
	
	/**
	 * The interface for handle HTTP request.
	 * @author thettian
	 *
	 */
	public interface RESTfulProcessor{
		public String execute(String uri);
	}
	
	public static class RESTfulServer {
		RESTfulProcessor processor ;
		public RESTfulServer(RESTfulProcessor processor){
			this.processor = processor;
		}
		
		/**
		 * create a Server with the protocol and port.
		 * @param protocol
		 * @param port
		 * @return
		 */
		public Server createServer(Protocol protocol, int port){
			Context context = new Context();
			context.getAttributes().put("processor", processor);
			
			return new Server(context, protocol, port, RESTfulServerResource.class);
		}
	}

}
