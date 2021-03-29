TradingMachine is a mini-trading system simulation made up of the following components:

* Market data feed: randomly builds ask/ bid prices and ask/ bid sizes for a given symbol and then publishes them onto a topic every X seconds.

* Orders feed: randomly builds market, limit and stop orders and then publishes them onto a queue every X seconds.

* FIX acceptor: listens on the market data and orders queues and provides order execution by a matching engine. It can deal with market, limit and stop orders. Market orders are always filled unless they're FOK, specifically, a market price will always be available from the market data while the quantity might not match the bid/ ask size. Limit and stop orders will be filled only if their limit/ stop price and quantity match the market data. Limit orders are subject to FOK too.

* FIX initiator: acts as an OMS, routing orders to the acceptor. It listens on the orders queue and forwards them to the FIX acceptor. If the acceptor replies with filled orders, then it publishes them on a topic.
Orders back-end store: subscribing to the orders topic, it stores executed and rejected orders them to MongoDB and MySql back-ends. The scripts to set up the MySQL database, tables and stored procedure, are provided in TradingServices/src/main/resources.

* Trade Monitor UI: subscribes to the orders and market data topics to show live execution/ rejection/ market data pluse the ones stored in the MongoDB repository. Furthermore, in the orders tab, it shows various order statistics.

It's built on Ubuntu 15.04 and Eclipse Mars, using the following technologies: Java 8, QuickFIX/J (FIX 5.0), Maven, ActiveMQ, MongoDB and MySql.
