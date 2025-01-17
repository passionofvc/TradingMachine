package com.projects.tradingMachine.server;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.projects.tradingMachine.utility.Utility;
import com.projects.tradingMachine.utility.database.creditCheck.CreditCheck;
import com.projects.tradingMachine.utility.database.creditCheck.ICreditCheck;
import com.projects.tradingMachine.utility.marketData.MarketData;

import quickfix.FieldNotFound;
import quickfix.LogUtil;
import quickfix.Message;
import quickfix.SessionID;
import quickfix.field.Account;
import quickfix.field.AvgPx;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.StopPx;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;

/**
 * Given an order and a link to the market data manager, it tries to match limit and stop orders based on the current market data. 
 * It always fills market orders unless they are FOK and no immediate fill is possible.
 * The market data provides bid, ask prices and sizes for a given symbol. 
 * */
public final class MatchingEngine implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);
	
	private static final AtomicInteger orderIdSequence = new AtomicInteger(0);
    private static final AtomicInteger execIdSequence = new AtomicInteger(0);
    private final MarketDataManager marketDataManager;
	private final quickfix.fix50.NewOrderSingle order;
	private final SessionID sessionID;
	private final ICreditCheck creditCheck;
    
	public MatchingEngine(final BasicDataSource creditCheckConnectionPool, final MarketDataManager marketDataManager, final quickfix.fix50.NewOrderSingle order, final SessionID sessionID) throws NumberFormatException, ClassNotFoundException, SQLException {
		this.marketDataManager = marketDataManager;
		this.order = order;
		this.sessionID = sessionID;
		creditCheck = new CreditCheck(creditCheckConnectionPool.getConnection());
	}
	
	@Override
	public void run() {
		try {
            final quickfix.fix50.ExecutionReport accept = new quickfix.fix50.ExecutionReport(
            		buildOrderID(), buildExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW), order.getSide(), 
            		new LeavesQty(order.getOrderQty().getValue()), new CumQty(0));
            
            accept.set(order.getClOrdID());
            accept.set(order.getSymbol());
            accept.set(new Text("new order"));
            //注文受付通知
            Utility.sendMessage(sessionID, accept);
            
            //try to fill now.
            final PriceQuantity priceQuantity = findPriceAndQuantity(order, 100);
            if (priceQuantity != null) {
            	if (!creditCheck.hasEnoughCredit(priceQuantity.getValue())) 
            		sendReject(order, sessionID, true);
            	else {
            		//約定通知
            		final quickfix.fix50.ExecutionReport executionReport = new quickfix.fix50.ExecutionReport(
                            buildOrderID(), buildExecID(), new ExecType(ExecType.FILL), new OrdStatus(
                                    OrdStatus.FILLED), order.getSide(), new LeavesQty(0), new CumQty(priceQuantity.getQuantity()));
                    executionReport.set(order.getClOrdID());
                    executionReport.set(order.getSymbol());
                    executionReport.set(order.getOrderQty());
                    executionReport.set(new Text(String.valueOf(priceQuantity.getMarketDataId())));
                    executionReport.set(new LastQty(priceQuantity.getQuantity()));
                    executionReport.set(new LastPx(priceQuantity.getPrice()));
                    executionReport.set(new AvgPx(priceQuantity.getPrice()));
                    creditCheck.setCredit(-priceQuantity.getValue());
                    Utility.sendMessage(sessionID, executionReport);
            	}
            }
            else //order rejected.
            	sendReject(order, sessionID, false);
        } catch (final Exception e) {
            LogUtil.logThrowable(sessionID, e.getMessage(), e);
        }
		finally {
			try {
				creditCheck.closeConnection();
			} catch (final SQLException e) {
				log.warn("Unable to close credit check database connection:\n"+e.getMessage());
			}
		}
	}
	
	private static void sendReject(final quickfix.fix50.NewOrderSingle  order, final SessionID sessionID, final boolean creditCheckFailed) throws FieldNotFound {
		final quickfix.fix50.ExecutionReport executionReport = new quickfix.fix50.ExecutionReport(
                buildOrderID(), buildExecID(), new ExecType(ExecType.REJECTED), new OrdStatus(OrdStatus.REJECTED), 
                order.getSide(), new LeavesQty(order.getOrderQty().getValue()), new CumQty(0));
    	executionReport.set(order.getClOrdID());
    	executionReport.set(new Text("000000000000"));
    	if (creditCheckFailed) {
            executionReport.set(new Account("Failed Credit Check"));//indicates not enough credit.
    	}
    	Utility.sendMessage(sessionID, executionReport);
	}
	
	private PriceQuantity findPriceAndQuantity(final quickfix.fix50.NewOrderSingle order, final int maxNrTrials) throws FieldNotFound, InterruptedException {
		int counter = 0;
		final int orderQuantity = (int)(order.getOrderQty().getValue());
		switch(order.getChar(OrdType.FIELD)) {
		case OrdType.LIMIT: //指値
			final double limitPrice = order.getDouble(Price.FIELD);
        	//loop until the limit order price is executable.
        	final char limitOrderSide = order.getChar(Side.FIELD);
        	while(counter < maxNrTrials) {
        		final PriceQuantity marketPriceQuantity = getMarketPriceQuantity(order);
        		if (
        			 (
        				   (limitOrderSide == Side.BUY && Double.compare(marketPriceQuantity.getPrice(), limitPrice) < 0)   //買注文の価格＞＝最良売気配
                        || (limitOrderSide == Side.SELL && Double.compare(marketPriceQuantity.getPrice(), limitPrice) > 0)) //売注文の価格＜＝最良買気配
        			  && (orderQuantity >= marketPriceQuantity.getQuantity())  //売買数量＞＝最良気配数量
        		){
        			log.info("Found filling price/ quantity for limit order, market price: "+marketPriceQuantity.getPrice()+", "
        					+ "limit price: "+limitPrice+", quantity: "+orderQuantity);
        			return new PriceQuantity(marketPriceQuantity.getPrice(), orderQuantity, marketPriceQuantity.getMarketDataId());
        		}
        		else {
        			//FOK
        			if (order.getChar(TimeInForce.FIELD) == TimeInForce.FILL_OR_KILL) {
        				return null;
        			}
        			log.debug("Looping to find filling price for limit order, market price: "+marketPriceQuantity.getQuantity()+", limit price: "+limitPrice);
        			Thread.sleep(500);
        			counter++;
        		}
        	}
        	return null; //price/ quantity not found.
		case OrdType.STOP: //逆指値
			final double stopPrice = order.getDouble(StopPx.FIELD);
    		//loop until the stop order price is executable.
        	final char stopOrderSide = order.getChar(Side.FIELD);
        	while(counter < maxNrTrials) {
        		//最良気配のみ
        		final PriceQuantity marketPriceQuantity = getMarketPriceQuantity(order);
        		if (
        			(   (stopOrderSide == Side.BUY && Double.compare(marketPriceQuantity.getPrice(), stopPrice) > 0)
                     || (stopOrderSide == Side.SELL && Double.compare(marketPriceQuantity.getPrice(), stopPrice) < 0)
                    ) 
        			&& (orderQuantity >= marketPriceQuantity.getQuantity())
        		) {
        			log.info("Found filling price for stop order, market price: "+marketPriceQuantity.getPrice()+", stop price: "+stopPrice);
        			return new PriceQuantity(marketPriceQuantity.getPrice(), orderQuantity, marketPriceQuantity.getMarketDataId());
        		}
        		else {
        			log.debug("Looping to find filling price for stop order, market price: "+marketPriceQuantity.getPrice()+", stop price: "+stopPrice);
        			Thread.sleep(500);
        			counter++;
        		}
        	}
        	return null; //price/ quantity not found.
        default:  //成行
        	final PriceQuantity marketPriceQuantity = getMarketPriceQuantity(order);
        	if (orderQuantity > marketPriceQuantity.getQuantity() && order.getChar(TimeInForce.FIELD) == TimeInForce.FILL_OR_KILL) {
            	return null;
        	}
            return new PriceQuantity(marketPriceQuantity.getPrice(), orderQuantity, marketPriceQuantity.getMarketDataId());
		}
    }
	
	private static class PriceQuantity {
		private final double price;
		private final double quantity;
		private final String marketDataId;
		
		public PriceQuantity(final double price, final double quantity, final String marketDataId) {
			this.price = price;
			this.quantity = quantity;
			this.marketDataId = marketDataId;
		}
		
		public double getPrice() {
			return price;
		}
		
		public double getQuantity() {
			return quantity;
		}
		
		public String getMarketDataId() {
			return marketDataId;
		}
		
		public double getValue() {
			return price * quantity;
		}
		
	}
	
	private PriceQuantity getMarketPriceQuantity(final Message message) throws FieldNotFound {
		final MarketData marketData = marketDataManager.get(message.getString(Symbol.FIELD));
		switch (message.getChar(Side.FIELD)) {
			case Side.BUY:  //買
				return new PriceQuantity(marketData.getAsk(), marketData.getAskSize(), marketData.getID());
			case Side.SELL: //売
				return new PriceQuantity(marketData.getBid(), marketData.getBidSize(), marketData.getID());
			default:
				throw new RuntimeException("Invalid order side: " + message.getChar(Side.FIELD));
		}
	}
	
	private static OrderID buildOrderID() {
        return new OrderID(String.valueOf(orderIdSequence.incrementAndGet()));
    }

    private static ExecID buildExecID() {
        return new ExecID(String.valueOf(execIdSequence.incrementAndGet()));
    }
}