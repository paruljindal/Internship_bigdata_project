cat load_current_recharge_events_hly.sh
ORACLE_HOME=/oracle/12c/product/12.1.0/client; export ORACLE_HOME
ORACLE_BASE=/oracle/12c; export ORACLE_BASE
LD_LIBRARY_PATH=/oracle/12c/product/12.1.0/client/lib; export LD_LIBRARY_PATH
PATH=/usr/sbin:$PATH; export PATH
PATH=$ORACLE_HOME/bin:/home/oracle/scripts:$PATH; export PATH
CLASSPATH=$ORACLE_HOME/JRE:$ORACLE_HOME/jlib:$ORACLE_HOME/rdbms/jlib; export CLASSPATH
/absand/rf1/airtel/cep/butterfly/cep_tech/bin/recharge_events_graphs/bin/python /absand/rf1/airtel/cep/butterfly/cep_tech/bin/recharge_events_graphs/src/load_current_recharge_events_hly.py

