package gatewayfinder.apigatewayfinder.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import gatewayfinder.apigatewayfinder.util.ClientBuilderManager;
import gatewayfinder.apigatewayfinder.util.PropertyManager;

/**
 * Implementation of {@link:IDatabaseTransactionManagerSevice}.
 * Manages all DynamoDB database transactions.
 * 
 * @author theja.kotuwella
 *
 */
public class DatabaseTransactionManagerImpl implements IDatabaseTransactionManagerSevice{
	private  LambdaLogger logger = null;
	
	private final String ISBN_ATTRIBUTE 	= "isbn";	
	private static final String ISBN_COLUMN = "isbn";
	
	public DatabaseTransactionManagerImpl(LambdaLogger logger) {
		this.logger = logger;
	}
	
	public Map<String, Map<String, String>> retrieveFromDatabase(String isbn) {
		// Parameterising DB query
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, 
																	AttributeValue>();
		
		expressionAttributeValues.put(":" + ISBN_ATTRIBUTE, 
											new AttributeValue().withS(isbn));

		// Query expression
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
	            .withFilterExpression(ISBN_COLUMN + "= :" + ISBN_ATTRIBUTE)
	            .withExpressionAttributeValues(expressionAttributeValues);
		
		AmazonDynamoDB client = ClientBuilderManager.dynamoDBClient();
		
		// Change to the database table name dynamically 
		// with using the value read from the properties file
		DynamoDBMapperConfig config = new DynamoDBMapperConfig.Builder()
										.withTableNameOverride(DynamoDBMapperConfig.TableNameOverride
											.withTableNameReplacement(
												PropertyManager.DATABASE_TABLE_NAME_VALUE))
										.build();
		
		logger.log("DatabaseTransactionHandler: Retrieving from table: " 
								+ PropertyManager.DATABASE_TABLE_NAME_VALUE);
		
		// Mapper for the EO
		DynamoDBMapper mapper = new DynamoDBMapper(client, config);

		List<BookInventory> scanResult = mapper.parallelScan(BookInventory.class, 
															scanExpression, 1);
		
		Map<String, Map<String, String>> result = new HashMap<String, Map<String
																	, String>>();
		// There will be only one in this case ideally
		for (BookInventory book : scanResult) {
			Map<String, String> bookMetadata = new HashMap<String, String>();
			bookMetadata.put("ISBN", book.getIsbn());
			bookMetadata.put("Author", book.getAuthor());
			bookMetadata.put("Stored Location", book.getS3Path());
			
			logger.log("DatabaseTransactionHandler: ISBN: " + book.getIsbn());
			logger.log("DatabaseTransactionHandler: Author: " + book.getAuthor());
			logger.log("DatabaseTransactionHandler: Stored Location: " + book.getS3Path());
			
			result.put(book.getIsbn(), bookMetadata);
		}
		return result;
	}
	
	@DynamoDBTable(tableName = "book_inventory")
	public static class BookInventory {
	    private String isbn;
	    private String author;
	    private String s3path;
	    
	    @DynamoDBHashKey(attributeName=ISBN_COLUMN)
	    public String getIsbn() {
	        return isbn;
	    }

	    public void setIsbn(String isbn) {
	        this.isbn = isbn;
	    }
	    
	    @DynamoDBAttribute(attributeName = "author")
	    public String getAuthor() {
	        return author;
	    }

	    public void setAuthor(String author) {
	        this.author = author;
	    }
	    
		@DynamoDBAttribute(attributeName = "s3path")
		public String getS3Path() {
			return s3path;
		}

		public void setS3Path(String s3path) {
			this.s3path = s3path;
		}

		@Override
	    public String toString() {
	        return "AssetTag [ISBN=" + isbn + ", Author=" + author + ", S3Path=" + s3path + "]";
	    }
	}
}
