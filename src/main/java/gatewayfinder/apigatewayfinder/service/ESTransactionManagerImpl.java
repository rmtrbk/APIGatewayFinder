package gatewayfinder.apigatewayfinder.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import gatewayfinder.apigatewayfinder.util.ClientBuilderManager;

/**
 * Implementation of {@link:IESTransactionManagerSevice}.
 * Manages all Elastic Search transactions.
 * 
 * @author theja.kotuwella
 *
 */
public class ESTransactionManagerImpl implements IESTransactionManagerSevice {
	private  LambdaLogger logger = null;
	
	private static final String ES_ISBN_ATTRIBUTE_NAME = "isbn";
	
	public ESTransactionManagerImpl(LambdaLogger logger) {
		this.logger = logger;
	}
	
	public List<String> search(String index) {
		logger.log("ElasticsearchTransactionManager: search...");
		
		List<String> listOfHits 	= new ArrayList<>();

		SearchRequest searchRequest = new SearchRequest("rekognition-media");
		QueryBuilder tagQuery = QueryBuilders.matchQuery(ES_ISBN_ATTRIBUTE_NAME, 
															index);

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(tagQuery));

		searchRequest.source(searchSourceBuilder);

		try {
			logger.log("ElasticsearchTransactionManager: Searching for " 
									+ ES_ISBN_ATTRIBUTE_NAME + ": " + index);
			
			SearchResponse searchResponse = ClientBuilderManager.getElasticSearchClient()
												.search(searchRequest, 
													RequestOptions.DEFAULT);

			SearchHit[] results = searchResponse.getHits().getHits();
			for(SearchHit hit : results) {
				Map<String, Object> sourceAsMap = hit.getSourceAsMap();
				
				listOfHits.add(sourceAsMap.get(ES_ISBN_ATTRIBUTE_NAME).toString());
			}
			logger.log("ElasticsearchTransactionManager: Search results found: " 
															+ listOfHits.size());

		} catch(ElasticsearchException e) {
			logger.log("ElasticsearchTransactionManager: " + e.getMessage());
			e.printStackTrace();

		} catch (IOException e) {
			logger.log(e.getMessage());
			
		} finally {
			try {
				ClientBuilderManager.getElasticSearchClient().close();
				logger.log("ElasticsearchTransactionManager: ES Client disconnected.");
				
			} catch (IOException e) {
				logger.log("ElasticsearchTransactionManager: " + e.getMessage());
				e.printStackTrace();
			}
		}
		logger.log("ElasticsearchTransactionManager: search completed.");
		return listOfHits;
	}
}
