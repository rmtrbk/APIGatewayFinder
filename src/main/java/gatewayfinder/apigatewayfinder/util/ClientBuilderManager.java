package gatewayfinder.apigatewayfinder.util;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Builds the AWS clients.
 * 
 * @author theja.kotuwella
 *
 */
public class ClientBuilderManager {

	private static RestHighLevelClient elasticsearchClient 	= null;
	private static AmazonDynamoDB dynamoDBclient = null;
	private static AmazonS3 s3Client 	= null;
	private static int SOCKET_TIMEOUT 	= 60000;

	public static RestHighLevelClient getElasticSearchClient() {
		if(elasticsearchClient == null) {
			AWS4Signer signer = new AWS4Signer();
			signer.setServiceName(PropertyManager.ELASTICSEARCH_SERVICE_NAME_VALUE);
			signer.setRegionName(Regions.AP_SOUTHEAST_2.getName());

			HttpRequestInterceptor interceptor 
			= new AWSRequestSigningApacheInterceptor(
					PropertyManager.ELASTICSEARCH_SERVICE_NAME_VALUE, 
					signer, 
					getcredentials());

			elasticsearchClient = new RestHighLevelClient(
					RestClient.builder(HttpHost.create(
							PropertyManager.ELASTICSEARCH_ENDPOINT_VALUE))
					.setHttpClientConfigCallback(
							hacb -> hacb.addInterceptorLast(interceptor)));
		}
		return elasticsearchClient;
	}

	private static AWSStaticCredentialsProvider getcredentials() {
		AWSCredentials credentials 	= new BasicAWSCredentials(
											PropertyManager.ACCESS_KEY_ATTRIBUTE,
											PropertyManager.SECRET_ATTRIBUTE);

		AWSStaticCredentialsProvider credProv = new AWSStaticCredentialsProvider(credentials);

		return credProv;
	}

	public static AmazonDynamoDB dynamoDBClient() {
		if(dynamoDBclient == null) {
			dynamoDBclient = AmazonDynamoDBClientBuilder.standard()
					.withRegion(Regions.AP_SOUTHEAST_2)
					.build();
		}
		return dynamoDBclient;
	}

	public static AmazonS3 s3Client() {
		if(s3Client == null) {
			try {
				ClientConfiguration clientConfig = new ClientConfiguration();
				clientConfig.setSocketTimeout(SOCKET_TIMEOUT);

				s3Client = AmazonS3ClientBuilder.standard()
						.withRegion(Regions.AP_SOUTHEAST_2)
						.build();

			} catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		return s3Client;
	}
}
