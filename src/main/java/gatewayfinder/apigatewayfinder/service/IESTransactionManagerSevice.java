package gatewayfinder.apigatewayfinder.service;

import java.util.List;

/**
 * Elastic Search transaction management interface.
 * 
 * @author theja.kotuwella
 *
 */
public interface IESTransactionManagerSevice {
	List<String> search(String index);
}
