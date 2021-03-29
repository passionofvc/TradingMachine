#mvn -f TradingMachineParentPom.xml compile
mvn -f TradingMachineParentPom.xml clean package install dependency:copy-dependencies
#mvn -f TradingMachineParentPom.xml install


