package com.projects.tradingMachine.services.database.noSql;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.projects.tradingMachine.utility.database.DatabaseProperties;

public final class MongoDBConnection implements AutoCloseable {
	private static Logger logger = LoggerFactory.getLogger(MongoDBConnection.class);
	
	private final MongoClient mongoClient;
	private final MongoDatabase mongoDatabase;
	
	public MongoDBConnection(final DatabaseProperties databaseProperties) {
		String username = databaseProperties.getUserName();
		String password = databaseProperties.getPassword();
		String authDatabase   = databaseProperties.getDatabaseName();
		MongoCredential credential = MongoCredential.createScramSha1Credential(username, authDatabase, password.toCharArray());
		final List<MongoCredential> credentials = new ArrayList<>();
		credentials.add(credential);
		ServerAddress serverAddress = new ServerAddress(databaseProperties.getHost(), databaseProperties.getPort());
		mongoClient = new MongoClient(serverAddress, credentials);
		mongoDatabase = mongoClient.getDatabase(databaseProperties.getDatabaseName());
	}
	
	@Override
	public void close() throws Exception {
		if (mongoClient != null) {
			mongoClient.close();
			logger.info("Connection closed.");
		}	
	}
	
	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}
}