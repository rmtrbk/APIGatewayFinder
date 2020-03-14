package gatewayfinder.apigatewayfinder.event;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import gatewayfinder.apigatewayfinder.service.DatabaseTransactionManagerImpl;
import gatewayfinder.apigatewayfinder.service.ESTransactionManagerImpl;
import gatewayfinder.apigatewayfinder.util.PropertyManager;

/**
 * Event driven class for API Gateway event. 
 * Any API Gateway event triggers a notification here and takes the next action 
 * from there.
 * 
 * @author theja.kotuwella
 *
 */
public class BookSearchRequestEventHandler 
	implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	LambdaLogger logger = null;
	
	private static final String REST_REQ_PARAM_ISBN 	= "isbn";
	private static final String REST_RES_SEARCHED_TERM	= "Searched Term";

	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
		logger = context.getLogger();
		PropertyManager.loadProperties();

		String isbn = "";

		Map<String, String> queryStringParams = event.getQueryStringParameters();

		// Pull parameters from REST call
		if (queryStringParams != null) {
			if (queryStringParams.get(REST_REQ_PARAM_ISBN) != null) {
				isbn = queryStringParams.get(REST_REQ_PARAM_ISBN);
				logger.log("APIGatewayProxyResponseEvent:" + REST_REQ_PARAM_ISBN 
															+ ": " 
															+ isbn);
			}
		}
		
		// Get the hits from Elasticsearch
		List<String> resultsFromES = searchInES(isbn);
		
		// Find the assets in DB
		Map<String, Map<String, String>> resultsFromDynDB = queryInDynamoDB(resultsFromES);
		
		// Put the search results in the response body
		Map<String, String> extraValues = new HashMap<String, String>();
		extraValues.put(REST_RES_SEARCHED_TERM, isbn);
		Map<String, String> responseBody = organiseResultsForResponseBody(resultsFromDynDB, extraValues);

		return createResponse(responseBody);
	}
	
	private List<String> searchInES(String isbn){
		ESTransactionManagerImpl estm = new ESTransactionManagerImpl(logger);
		List<String> results = estm.search(isbn);
		
		return results;
	}
	
	private Map<String, Map<String, String>> queryInDynamoDB(List<String> listOfValues) {
		DatabaseTransactionManagerImpl dth = new DatabaseTransactionManagerImpl(logger);
		Map<String, Map<String, String>> results = new HashMap<>();
		
		listOfValues.forEach(assetUUID -> 
							results.putAll(dth.retrieveFromDatabase(assetUUID)));

		return results;
	}
	
	private Map<String, String> organiseResultsForResponseBody(Map<String, Map<String, String>> values, Map<String, String> extraValues) {
		Map<String, String> responseBody = new HashMap<String, String>();
		
		values.forEach((outerKey, metadata) -> {
			metadata.forEach((innerKey, entry) -> {
				responseBody.put(innerKey + ": ", entry);
			});
		});
		
		return responseBody;
	}
	
	private APIGatewayProxyResponseEvent createResponse(Map<String, String> responseBody) {
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		
		// Set the response header
		response.setHeaders(Collections.singletonMap("book-search-header", "books"));
		response.setStatusCode(200);
		String responseBodyString = new JSONObject(responseBody).toJSONString();
		response.setBody(responseBodyString);
		return response;
	}
}
