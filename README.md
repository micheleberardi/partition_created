# partition_created

command for table log_history startdate finishdate
./partition_created.sh user password basename log_history YYYY-MM YYYY-MM

command for table opt_a_historical startdate finishdate
./partition_created1.sh user password basename opt_a_historical YYYY-MM YYYY-MM

command for table log_trace startdate finishdate
./partition_created1.sh user password basename log_trace YYYY-MM-DD YYYY-MM-DD

command for table ts_conversion startdate finishdate
./partition_created1.sh user password basename ts_conversion YYYY-MM-DD YYYY-MM-DD
