package gatewayfinder.apigatewayfinder.service;

import java.util.Map;

/**
 * Database transaction management interface.
 * 
 * @author theja.kotuwella
 *
 */
public interface IDatabaseTransactionManagerSevice {
	Map<String, Map<String, String>> retrieveFromDatabase(String isbn);
}
