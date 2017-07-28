<?php
 error_reporting(E_ALL | E_STRICT) ;
ini_set('display_errors', 'On');
/**
 * MySQL Apache Log Import
 *
 * This script imports apache combined log format logs into MySQL, so you can use standard SQL commands
 * to query your logs. For usage, please run the script with no arguments.
 * 
 * 
 *
 * 
 * @requires PHP 5.X
 * @requires MySQL 5.X
 *
 */
 
define('VERSION', '1.0.2');
define('TMP_FILE', '/tmp/log_import.tmp');
$pathFile = TMP_FILE; 
//
// STEP 1: GET CMD LINE ARGS
//
 
// command line arguments; check below for usage
$cmdArgs = getopt('d:t:h:u:p:cxf');
 
// check args
if (!(isset($cmdArgs['d']) && strlen($cmdArgs['d']) > 0 && isset($cmdArgs['t']) && strlen($cmdArgs['t']) > 0))
	displayUsage();
 
// connect to mysql database
$dbHost = isset($cmdArgs['h']) ? $cmdArgs['h'] : ini_get("mysqli.default_host");
$dbUser = isset($cmdArgs['u']) ? $cmdArgs['u'] : ini_get("mysqli.default_user");
$dbPass = isset($cmdArgs['p']) ? $cmdArgs['p'] : ini_get("mysqli.default_pw");
$dbTable = $cmdArgs['t'];
$dbName = $cmdArgs['d'];
$mysqli = mysqli_init();
$mysqli->options(MYSQLI_OPT_LOCAL_INFILE, true);
$mysqli->real_connect($dbHost, $dbUser, $dbPass, $dbName);
 
// check to see if we need to drop and/or create the table
$quotedDbTable = dbQuoteIdentifier($mysqli, $dbTable, true);
if (isset($cmdArgs['x'])) {
	$cmdArgs['c'] = true; 				// -x implies -c
	$queryResult = dbQuery($mysqli, "DROP TABLE IF EXISTS `{$quotedDbTable}`");
}
$quotedDbTable = dbQuoteValue($mysqli, $dbTable, true);
$queryResult = dbQuery($mysqli, "SHOW TABLES LIKE '{$quotedDbTable}'");


if ($queryResult->num_rows != 1) {
	if(isset($cmdArgs['c'])) {
		dbCreateTable($mysqli, $dbTable);
	} else {
		die ("Database table '{$dbTable}' does not exist. Please rerun the script with the -c option to create it.\n");
	}
}
 
//
// STEP 2: COPY DATA INTO TAB-DELIMITED FILE
//
 
// open the temp CSV file for copying data
$tmpFile = fopen(TMP_FILE, 'w') or die("Error open file");
// read each line of STDIN and process a log
$checkDb = isset($cmdArgs['f']) ? false : true;
while (!feof(STDIN)) {
	$line = fgets(STDIN);
 
	if (empty($line))
		continue;
 
	$results = processLine($line);
 
	// check the first and last entries; print an error if something went wrong
	if (empty($results['fullString']) || !is_numeric($results['status'])) {
		echo "Error! Could not interpret line: ".$line;
		continue;
	}
 
	// convert entries to database format. NOTE: doing the timestamp conversion this way converts
	// each entry to the local timezone on the local box. Stupid MySQL doesn't support storing a timezone
	// with a timestamp, so we covert everything from the web server's timezone to the local box's timezone,
	// and store that.
			$results['date'] = str_replace('/', ' ', $results['date']);
			$logTimestamp = strtotime("{$results['date']} {$results['time']} {$results['timezone']}");
			$sqlTimestamp = date('Y-m-d H:i:s', $logTimestamp);	
			$results['bytes'] = is_numeric($results['bytes']) ? $results['bytes'] : '0';
 
	// run a $mysqli->escape_string() on all the strings to put into the database. We don't want to use
	// dbQuoteValue(), because that also adds quotes, which the LOAD DATA command interepts litterally
			$client_ip 		            = $mysqli->escape_string($results['client_ip']);                 
			$client_port 			    = $mysqli->escape_string($results['client_port']);
			$frontend_name_transport    = $mysqli->escape_string($results['frontend_name_transport']); 
			$backend_name               = $mysqli->escape_string($results['backend_name']);
			$server_name                = $mysqli->escape_string($results['server_name']);
			$Tq 			 			= $mysqli->escape_string($results['Tq']);
			$Tw                       	= $mysqli->escape_string($results['Tw']);
			$Tc			  				= $mysqli->escape_string($results['Tc']);
			$Tr			  		   		= $mysqli->escape_string($results['Tr']);
			$Tt   				   		= $mysqli->escape_string($results['Tt']);
			$status                     = $mysqli->escape_string($results['status']);
			$bytes                      = $mysqli->escape_string($results['bytes']);
			$CC                        	= $mysqli->escape_string($results['CC']);
			$CS                        	= $mysqli->escape_string($results['CS']);
			$termination_state_cookie  	= $mysqli->escape_string($results['termination_state_cookie']);
			$actconn    		  		= $mysqli->escape_string($results['actconn']);
			$feconn    		  			= $mysqli->escape_string($results['feconn']);
			$beconn     		  		= $mysqli->escape_string($results['beconn']);
			$srv_conn   		  		= $mysqli->escape_string($results['srv_conn']);
			$retries   		  			= $mysqli->escape_string($results['retries']);
			$srv_queue		 			= $mysqli->escape_string($results['srv_queue']);
			$backend_queue        	 	= $mysqli->escape_string($results['backend_queue']);
			$captured_request_headers  	= $mysqli->escape_string($results['captured_request_headers']);
			$captured_response_headers 	= $mysqli->escape_string($results['captured_response_headers']);
			$referral                  	= $mysqli->escape_string($results['referral']);
			$Q_http_request            	= $mysqli->escape_string($results['Q_http_request']);
			$userAgent					= $mysqli->escape_string($results['userAgent']);
			$method                    	= $mysqli->escape_string($results['method']);

 
	// figure out if we should check the database for the first entry. This helps prevent 
	// duplicates. Use -f to override
	if ($checkDb) {
		$quotedDbTable = dbQuoteIdentifier($mysqli, $dbTable);
		
		$sql = <<<QQ
			SELECT TRUE FROM `{$quotedDbTable}`
			WHERE 
		    client_ip   	= '{$client_ip}' AND
		    client_port 	= '{$client_port}' AND
		    status 			= '{$status}' AND
		    bytes 			= '{$bytes}'
QQ;
 
		$queryResult = dbQuery($mysqli, $sql);
		if ($queryResult->num_rows > 0)
			die("Skipping file; the first entry of this log file already appears to be stored in the database. Use -f to override.\n");
 
		// check only the first row
		$checkDb = false;
	}
 
	$logString = "{$client_ip}\t{$client_port}\t{$frontend_name_transport}\t{$backend_name}\t{$server_name}\t{$Tq}\t{$Tw}\t{$Tc}\t{$Tr}\t{$Tt}\t{$status}\t{$bytes}\t{$CC}\t{$CS}\t{$termination_state_cookie}\t{$actconn}\t{$feconn}\t{$beconn}\t{$srv_conn}\t{$retries}\t{$srv_queue}\t{$backend_queue}\t{$captured_request_headers}\t{$captured_response_headers}\t{$referral}\t{$Q_http_request}\t{$userAgent}\t{$method}\n";

	
	fwrite($tmpFile, $logString);
 
}
fclose($tmpFile);
 
//
// STEP 3: COPY TAB-DELIMITED FILE INTO DB
//
 
// load data into database
$quotedDbTable = dbQuoteIdentifier($mysqli, $dbTable);
$sql = <<<QQ
LOAD DATA LOCAL INFILE '{$pathFile}' INTO TABLE {$quotedDbTable} 
	FIELDS TERMINATED BY '\t' 
	LINES TERMINATED BY '\n';
QQ;

dbQuery($mysqli, $sql);
 
// delete the tmp file after importing
unlink(TMP_FILE);
 
$mysqli->close();
 
/*******************************************************************************
 *************************      INTERNAL FUNCTIONS     *************************
 *******************************************************************************/
 
/**
 * processLine(): processes a line of a log file, returning an associative array
 * with the component parts
 *
 * @param string $line the line of the log
 * @return array associative array of values from log file
 *
 */
function processLine($line) {
	$matches = array();
 
	// process the string. This regular expression was adapted from http://oreilly.com/catalog/perlwsmng/chapter/ch08.html
	preg_match('/^(\S+) (\S+) (\S+) (\S+) (\S+) (\d+\.\d+\.\d+\.\d+):(\d+) \[([^:]+):(\d+:\d+:\d+)\.(\d+)\] (\S+) (\w+)\/[<]*(\w+)[>]* (-*\d+)\/(-*\d+)\/(-*\d+)\/(-*\d+)\/(-*\d+) (\S+) (\S+) (\S+) (\S+) (\S+) (\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+) (\d+)\/(\d+) \{([^{]+)\} \"[<]*([^"]+[^>])[>]*\"$/', $line, $matches);
	$r = explode('|',$matches[31]);
	$referral = isset($r[1]) ? $r[1] : '-';
	
	if (isset($matches[0])) {
		return array('fullString' 	=> $matches[0],
			'client_ip'		        	=>$matches[6],                 
			'client_port'			    =>$matches[7],              
			'date'  			   		=>$matches[8],
			'time'						=>$matches[9],
			'timezone'	                =>$matches[10],
			'frontend_name_transport'   =>$matches[11], 
			'backend_name'              =>$matches[12],
			'server_name'               =>$matches[13],
			'Tq'			 			=>$matches[14],
			'Tw'                       	=>$matches[15],
			'Tc'			  			=>$matches[16],
			'Tr'			  		   	=>$matches[17],
			'Tt'    				   	=>$matches[18],
			'status'                    =>$matches[19],
			'bytes'                     =>$matches[20],
			'CC'                        =>$matches[21],
			'CS'                        =>$matches[22],
			'termination_state_cookie'  =>$matches[23],
			'actconn'    		  		=>$matches[24],
			'feconn'    		  		=>$matches[25],
			'beconn'     		  		=>$matches[26],
			'srv_conn'   		  		=>$matches[27],
			'retries'    		  		=>$matches[28],
			'srv_queue'		 			=>$matches[29],
			'backend_queue'        	 	=>$matches[30],
			'captured_request_headers'  =>'-',
			'captured_response_headers' =>'-',
			'referral'                  =>$referral,
			'Q_http_request'            =>$matches[31],
			'userAgent'					=>$r[1],	
			'method'                    =>$matches[32] 
		);
	} else {
		return array();
	}
	
}
/* new function  */
function dbQuoteIdentifier($mysqli, $dbTable, $flag=false) {
	$result = $mysqli->query("SHOW TABLES LIKE '".$dbTable."'");
	$tableName = mysqli_fetch_row($result);
	return $tableName[0];
}
function dbQuery($mysqli, $sql){

	$result = $mysqli->query($sql);
	echo $mysqli->error;
	return $result;
}
function dbQuoteValue($mysqli, $param){
	$result = $mysqli->query("SHOW TABLES LIKE '".$param."'");
	$tableName = mysqli_fetch_row($result);
	return $tableName[0];
}
function dbCreateTable($mysqli, $dbTable){
	$mysqli->query("CREATE TABLE `".$dbTable."`(
					`client_ip` VARCHAR(32) NOT NULL,
					`client_port` int(8) NOT NULL,
					`frontend_name_transport` VARCHAR(64) NOT NULL, 
					`backend_name` VARCHAR(64) NOT NULL, 
					`server_name` VARCHAR(64) NOT NULL,
					`Tq` int(12) NOT NULL,
					`Tw` int(12) NOT NULL,
					`Tc` int(12) NOT NULL,
					`Tr` int(12) NOT NULL,
					`Tt` int(12) NOT NULL,
					`status` int(4) NOT NULL,
					`bytes` int(32) NOT NULL,
					`CC` VARCHAR(64) NOT NULL,
					`CS` VARCHAR(64) NOT NULL,
					`termination_state_cookie` VARCHAR(64) NOT NULL,
					`actconn` int(32) NOT NULL,
					`feconn` int(32) NOT NULL,
					`beconn` int(32) NOT NULL,
					`srv_conn` int(32) NOT NULL,
					`retries` int(32) NOT NULL,
					`srv_queue` int(32) NOT NULL,
					`backend_queue` int(32) NOT NULL,
					`captured_request_headers` VARCHAR(64) NOT NULL,
					`captured_response_headers` VARCHAR(64) NOT NULL,
					`referral` VARCHAR(255) NOT NULL,
					`Q_http_request` VARCHAR(255) NOT NULL,
					`userAgent` VARCHAR(255) NOT NULL,
					`method` VARCHAR(255) NOT NULL,
					`id` INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY
					)");
} 


/**
 * displayUsage(): display a usage message and exit
 *
 */
function displayUsage() {
	$version = VERSION;
	echo <<<QQ
{$_SERVER['SCRIPT_NAME']} v{$version}: Imports an Apache combined log into a MySQL database.
Usage: mysql_httpd_log_import -d <database name> -t <table name> [options] < log_file_name
 -d <database name> The database to use; required
 -t <table name>    The name of the table in which to insert data; required
 -h <host name>     The host to connect to; default is localhost
 -u <username>      The user to connect as
 -p <password>      The user's password
 -c                 Create table if it doesn't exist
 -x                 Drop the existing table if it exists. Implies -c
 -f                 Force load; skip the duplicate check. By default, the software exits if
                    the first entry in a given file already exists in the database
 
QQ;
 
exit;
}