package com.projects.tradingMachine.services.database.noSql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.projects.tradingMachine.services.database.DataManager;
import com.projects.tradingMachine.utility.Utility;
import com.projects.tradingMachine.utility.database.DatabaseProperties;
import com.projects.tradingMachine.utility.marketData.MarketData;
import com.projects.tradingMachine.utility.order.OrderSide;
import com.projects.tradingMachine.utility.order.OrderTimeInForce;
import com.projects.tradingMachine.utility.order.OrderType;
import com.projects.tradingMachine.utility.order.SimpleOrder;

public final class MongoDBManager implements DataManager {
	private static Logger logger = LoggerFactory.getLogger(MongoDBManager.class);
	
	private final MongoDBConnection mongoDBConnection;
	private final MongoCollection<Document> executedOrdersCollection;
	private final MongoCollection<Document> marketDataCollection;
	
	public MongoDBManager(final MongoDBConnection mongoDBConnection, final String executedOrdersCollectionName, final String marketDataCollectionName) {
		this.mongoDBConnection = mongoDBConnection;
		executedOrdersCollection = mongoDBConnection.getMongoDatabase().getCollection(executedOrdersCollectionName);
		marketDataCollection = marketDataCollectionName == null ? null : mongoDBConnection.getMongoDatabase().getCollection(marketDataCollectionName);
	}
	
	public MongoDBManager(final MongoDBConnection mongoDBConnection, final String executedOrdersCollectionName) {
		this(mongoDBConnection, executedOrdersCollectionName, null);
	}

	@Override
	public void storeOrder(final SimpleOrder order) {
		executedOrdersCollection.replaceOne(new Document("FilledOrder", order.getID()), ConvertSimpleOrderToBSONDocument(order), 
				new UpdateOptions().upsert(true));
		logger.debug(order+" added to collection: "+executedOrdersCollection.toString());
	}
	
	@Override
	public List<SimpleOrder> getOrders(final Optional<OrderType> orderType) {
		long startTime = System.currentTimeMillis();
		final List<SimpleOrder> result = new ArrayList<SimpleOrder>();
		final MongoCursor<Document> cursor = orderType.isPresent() ? executedOrdersCollection.find(new Document("Type", orderType.get().toString())).iterator() : executedOrdersCollection.find().iterator();
		try {
		    while (cursor.hasNext()) {
		    	final Document doc = cursor.next();
		    	result.add(new SimpleOrder(doc.getString("ID"), doc.getString("Symbol"), doc.getInteger("Quantity"), 
		    		 OrderSide.fromString(doc.getString("Side")), OrderType.fromString(doc.getString("Type")), OrderTimeInForce.fromString(doc.getString("TimeInForce")), 
		    		 doc.getDouble("LimitPrice"), doc.getDouble("StopPrice"), doc.getDouble("Price"), doc.getString("OriginalID"), doc.getDate("StoreDate"), 
		    		 doc.getBoolean("IsRejected"), doc.getString("MarketDataID"), doc.getBoolean("IsCreditCheckFailed", false)));
		    }
		} finally {
		    cursor.close();
		}
		logger.info("Time taken to retrieve orders: "+(System.currentTimeMillis() - startTime)+" ms.");
		return result;
	}
	
	@Override
	public void storeMarketDataItems(final List<MarketData> marketDataItems, final boolean deleteFirst) {
		logger.debug("Starting to store "+ marketDataItems.size()+" MarketData items...");
		if (deleteFirst) 
			marketDataCollection.deleteMany(new Document());
		final List<Document> docs = new ArrayList<Document>();
		marketDataItems.forEach(marketDataItem -> docs.add(ConvertMarketDataToBSONDocument(marketDataItem)));
		marketDataCollection.insertMany(docs);
		logger.debug("Data stored successfully");
	}
	
	@Override
	public List<MarketData> getMarketData(final Optional<String> symbol) {
		long startTime = System.currentTimeMillis();
		final List<MarketData> result = new ArrayList<MarketData>();
		final MongoCursor<Document> cursor = symbol.isPresent() ? marketDataCollection.find(new Document("Symbol", symbol.get())).iterator() : marketDataCollection.find().iterator();
		try {
		    while (cursor.hasNext()) {
		    	final Document doc = cursor.next();
				result.add(new MarketData(doc.getString("ID"), doc.getString("Symbol"), doc.getDouble("Ask"), 
		    			doc.getDouble("Bid"), doc.getInteger("AskSize"), doc.getInteger("BidSize"), doc.getDate("QuoteTime")));
		    }
		} finally {
		    cursor.close();
		}
		logger.info("Time taken to retrieve orders: "+(System.currentTimeMillis() - startTime)+" ms.");
		return result;
	}

	@Override
	public void close() throws Exception {
		mongoDBConnection.close();
	}

	private static Document ConvertSimpleOrderToBSONDocument(final SimpleOrder order) {
		return new Document("ID", order.getID())
		        .append("Symbol", order.getSymbol())
		        .append("Quantity",order.getQuantity())
		        .append("Side", order.getSide().toString())
		        .append("Type", order.getType().toString())	
		        .append("TimeInForce", order.getTimeInForce().toString())
		        .append("LimitPrice", order.getLimit())
		        .append("StopPrice", order.getStop())
		        .append("Price", order.getAvgPx())
		        .append("OriginalID", order.getOriginalID())
				.append("StoreDate", new Date())
				.append("IsRejected", order.isRejected())
				.append("MarketDataID", order.getMarketDataID())
				.append("IsCreditCheckFailed", order.isCreditCheckFailed());
	}
	
	private static Document ConvertMarketDataToBSONDocument(final MarketData marketData) {
		return new Document("ID", marketData.getID()).
				append("Symbol", marketData.getSymbol())
		        .append("Ask", marketData.getAsk())
		        .append("Bid",marketData.getBid())
		        .append("AskSize", marketData.getAskSize())
		        .append("BidSize", marketData.getBidSize())	
		        .append("QuoteTime", marketData.getQuoteTime());
	}
	
	public static void main(final String[] args) throws NumberFormatException, Exception {
		final Properties p = Utility.getApplicationProperties("tradingMachineServices.properties"); 
		try(final DataManager mongoDBManager = new MongoDBManager(new MongoDBConnection(new DatabaseProperties(p.getProperty("mongoDB.host"), 
				Integer.valueOf(p.getProperty("mongoDB.port")), p.getProperty("mongoDB.database"))), p.getProperty("mongoDB.executedOrdersCollection"))) {
			//System.out.println(mongoDBManager.getOrders(Optional.of(OrderType.STOP)).stream().mapToDouble(SimpleOrder::getAvgPx).summaryStatistics());
			//mongoDBManager.getOrders(Optional.of(OrderType.LIMIT)).stream().map(SimpleOrder::getAvgPx).forEach(System.out::println);
			//System.out.println(mongoDBManager.getOrders(Optional.ofNullable(null)).stream().collect(Collectors.groupingBy(SimpleOrder::getType, Collectors.counting())));
		}
	}
}