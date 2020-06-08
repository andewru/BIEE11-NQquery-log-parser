<?php

/**
 * Скрипт может принимать первым параметром имя файла из директории $nqLogDir, тогда будет обрабатываться только этот файл
 * Скрипт без параметров обрабатывает все файлы в директории $nqLogDir
 */

$serverName = 'your-host';
$connectionOptions = [
	'Database'   => 'MD',
	'CharacterSet'  => 'UTF-8',
	//"Uid"=>'mssqlserverusername',   //not needed for win authentication
	//"PWD"=>'password',  //not needed for win authentication
];

/**
 * Директория с nqquery.log файлами, задавать с конечным слэшем
 * @var string
 */
$nqLogDir = str_replace("\\", "/", __DIR__."/download/downloaded-files/");


/**
 * Файл логирования ошибок работы скрипта
 * @var string
 */
$errorLog = str_replace("\\", "/", __DIR__.'/parse-errors.log');


/**
 * Паттерн захвата одной секции/записи из nqquery.log BI 11g (старая версия лога как txt)
 * @var string
 */
$regexp = '/\[([\d]{4}\-[\d]{2}\-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}\.[\d]{3})\+[\d]{2}:[\d]{2}\]\s\[OracleBIServerComponent\]\s\[TRACE:5\]\s\[USER-([\d]{1,2})\]\s\[\]\s\[ecid:\s([^\n]{1,60}),[0-9:]{1,30}\]\s\[tid:\s[a-z0-9]{8}\]\s\[requestid:\s([a-z0-9]{8})\]\s\[sessionid:\s([a-z0-9]{8})\]\s\[username:\s([a-zA-Z0-9_-]{1,40})\]\s([^\n]+)\s\[\[(.+)\]\]/siU';

/**
 * Pattern имени файла для поиска в $nqLogDir
 * 1591321629-nqquery*.log - искомые файлы
 * @var string
 */
$pattern = "/^[0-9]{5,20}-nqquery[-0-9a-z]{0,10}\.log$/siU";

// -------------------------------------------------------------------------------------------------------------
try {
	//включение логирования ошибок PHP в заданный файл, в него сообщения пишутся нарастающим итогом
	//error_reporting(E_ALL);
	ini_set('error_log', $errorLog ); //задаем файл в который будем писать, если файла нет, то он будет создан автоматически
	ini_set('log_errors', true); //включаем логирования ошибок PHP

	$startTime = microtime(true); //microtime as float value of seconds
	ini_set('memory_limit', '500M'); //увеличить лимит на выделение памяти
	$connection = getConnection($serverName, $connectionOptions); //выкинет Exception если не удалось подключиться к базе данных

	//----------------------------------------------------------------------------------
	$files = []; //массив для хранения полных путей обрабатываемых файлов


	if (!empty($argv[1])) {
		//если есть параметр, то работаем только для этого файла
		$files[] = $argv[1]; //первый параметр переданный скрипту - ожидаем имя файла из директории $nqLogDir
	} else {
		//если параметра нет, то читаем директорию $nqLogDir и обрабатываем файлы
		if(is_dir($nqLogDir) === true) {

			$dh = opendir($nqLogDir); //вернет resource, или FALSE если ошибка

			if( $dh  === false ) {
				throw new Exception("ERROR: opendir() for $nqLogDir failed!");
			}
			//перебор всех имен в директории - это могут быть как файлы так и директории, 
			while (($filename = readdir($dh)) !== false ) {

				//отбираем только файлы и удовлетворяющие условию
				if(@preg_match($pattern , $filename) === 1 && @filetype($nqLogDir.$filename) === 'file' ) {
				
					$files[] = $filename;
				}
			}
			closedir($dh);
		} else {
			throw new Exception("ERROR: $nqLogDir is not a Dir!");
		}
	}

	if(is_array($files) && count($files) > 0) {

		foreach ($files as $filename) {
			//doFile сама делает вывод и всех обработку ошибок
			doFile($nqLogDir, $filename, $regexp,$connection); //обработка одного файла, пасинг и загрузка в базу данных, вывод сообщений и ошибок, проверяет сначала реальность файла
			//echo $filename.PHP_EOL; //для теста
		}
	} else {
		echo 'INFO: No files to process.'.PHP_EOL;
	}

	//-------------------------------------------------------------------------------------
	closeConnection($connection);
	$endTime = microtime(true);
	$executeTime = $endTime - $startTime;
	$tmp = 'INFO: Script execute time: '.round($executeTime, 3).' seconds.';
	echo $tmp.PHP_EOL;
	error_log($tmp); //логирование в PHP error_log резюме по обработке файла, потом можно отключить

} catch (\Exception $e) {
	echo 'ERROR: '.$e->getMessage();
	error_log('ERROR: '.$e->getMessage()); // добавление в конец лог файл записи об ошибки для исключений. Не обязательно именно для PHP ошибки, т.к. любая PHP ошибка и так будет залогированна.
	exit();
}


//-------------------------------------------------------------------------------------------------

/**
 * Обрабатывает один файл nqquery лога из директории $nqLogDir,
 * читает файл, парсит, пишет результат в базу, переименовывает файл
 * в вид: [0-9]errors-[0-9]success_$filename
 * обрабатывает ошибки PHP и SQL, пишет error_log, выводит в терминал сообщения.
 * @param string директория файла $nqLogDir
 * @param string имя файла nqquery лога из директории $nqLogDir
 * @param string регулярное выражение для парсинга nqquery.log
 * @param connection MSSQL connection
 */
function doFile(string $nqLogDir , string $filename, string $regexp, $connection) : void {

	$fullFilename = $nqLogDir.$filename;
	
	if (file_exists($fullFilename) && is_readable ($fullFilename)) {

		echo 'Start for file: '.$filename.PHP_EOL;

		$file = file_get_contents($fullFilename); //читаем весь файл в переменную
		$isMatch = preg_match_all($regexp, $file, $matches, PREG_SET_ORDER);
		unset($file);//файл далее не нужен

		if ($isMatch === false) {
			$tmp = 'ERROR: preg_match_all() retuned FALSE for file: '.$filename;
			echo $tmp.PHP_EOL;
			error_log($tmp); //запишем в PHP error_log
		} if ($isMatch === 0) {
			$tmp = 'WARNING: no matches found in file: '.$filename;
			echo $tmp.PHP_EOL;
			error_log($tmp); //запишем в PHP error_log
		} else {
			/**
			 * Структура вложенного массива $matches
			 * [0] => полный текст всего выхваченного рег выражением, ниже будет заменено на его хеш md5([0])
			 * [1] => 2020-05-14T12:24:12.000 --datatime as ISO 8601, зону +03:00 убрал, т.к. мне именно нужно делать как CONVERT(DATETIME, '2020-05-14T12:24:12.000') при сохранении в MSSQL
			 * [2] => 20 //USER-20
			 * [3] => b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0 //ecid
			 * [4] => 632f0010 //requestid - уникальный идентификатор запроса BI
			 * [5] => 632f0000 //sessionid
			 * [6] => username //username
			 * [7] => разделитель + текст перед телом запроса - ниже форматируется для удобства чтения
			 * "##############################################" - начало запроса как SQL Request, logical request hash или "-------------------- сообщение:"  - для всех остальных секций
			 * [8] => текст - сообщение или запрос
			 */
			$inserted = 0; //insert rows count - передается в insertRow по ссылке!
			$insertStatuses = ['success' => 0, 'errors' => 0]; //Default set - количество по статусам вставки в базу строк, чтобы знать сколько было ошибочных вставок

			//форматирование результата парсинга для каждого элемента $matches (одно строка лога)
			foreach ($matches as &$value) {
				//замена [0] на его md5 хеш
				$value[0] = md5($value[0]);
				//Форматирование для [7]
				if($value[7] === '##############################################') {
					$value[7] = 'START - SQL Request, logical request hash:';
				} else {
					$value[7] = trim(trim($value[7], '-'));
					//обрежем под nvarchar(255) для БД
					$value[7] = substr($value[7], 0, 255);  // у меня латиница, поэтому substr здесь ок
				}
				$value[8] = trim(trim(trim(trim(trim($value[8], "\r\n"), "\n")), '-'));
				$value[9] = strtotime($value[1]); // int UNIX TIMESTAMP (секунды) на основе [1]

				// вставка результата в базу данных для каждой строки
				$insertRow = insertRow($connection, $value, $inserted); //'success' or formated sqlsrv_errors as string
				if($insertRow === 'success') {
					$insertStatuses['success']++; //если все ок, то должно быть = count($matches), только в этом случае обработку всего файла можно считать успешной и законченной!
				} else {
					$insertStatuses['errors']++; //количество ошибочных вставок (SQL ошибки) - если >0, то значит нужно разбираться почему!!!
					//echo $insertRow.PHP_EOL; //TODO можно добавить вывод в терминал ошибки sqlsrv_errors, но только в режиме отладки
				}
			}

			//вывод в теминал резюме по обработке файла
			$tmp = 'Finish for file: '.$filename.', '.count($matches).' matches found, '.$insertStatuses['success'].' successfully processed, '. $inserted.' rows inserted, '.$insertStatuses['errors'].' SQL errors.';
			echo $tmp.PHP_EOL;
			if($insertStatuses['errors'] > 0) {
				error_log( 'ERROR: '.$tmp); //запишем в PHP error_log если были ошибки SQL
			} else {
				error_log( 'INFO: '.$tmp); //логирование в PHP error_log резюме по обработке файла, потом можно отключить
			}

			//переименуем уже обработанный файл
			if(rename($fullFilename, str_replace("\\", "/", $nqLogDir.$insertStatuses['errors'].'errors-'.$insertStatuses['success'].'success_'.$filename)) === false) {
				$tmp = 'File rename failed for '.$filename;
				echo $tmp.PHP_EOL;
				error_log('ERROR: '.$tmp); //запишем в PHP error_log, если ошибка переименования
			}
			
		}
	} else {
		$tmp = 'WARNING: file '.$fullFilename.' not found in directory.';
		echo $tmp.PHP_EOL;
		error_log($tmp); //запишем в PHP error_log
	}
}


/**
 * Вставка одной строки лога из индексного массива значений
 * @param $connection to MSSQL
 * @param array $params array to insert values as 0,1,2,3,4,5,6,7,8,9
 * @param int &$inserted rows count by link
 * @return string 'success' or formated sqlsrv_errors as string
 */
function insertRow($connection, $params, &$inserted) : string {

	/**
	 * Структура массива $params для записи в TABLE [BI_NQQUERY_LOG]
	 * [Id_Nqquery_Log] nchar(32) not null	--[0] => '25ebd9a574ffbcc08e7da214e55397ef' string: md5 hash записи из лога, для вычисления дубликатов при повторном парсинге тех же файлов
	 * [LogDateTime] datetime not null		--[1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
	 * [User] tinyint not null				--[2] => 23 int: код user для записи из лога от USER-23 - этот код можно использовать как флаг типа записи
	 * [ExecuteId] nvarchar(60) not null	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0' string код 'ecid' для записи из лога - execute id, уникален (в рамках одной сессии точно), в рамках одного ecid может быть несколько requestid
	 * [RequestId] nchar(8) not null		--[4] => '632f0010' string: requestid - уникальный идентификатор запроса BI для записи из лога, идентифицирует блок записей относящихся к одному запросу в рамках одного ecid
	 * [SessionId] nchar(8) not null		--[5] => '632f0000' string: sessionid - идентификатор сессии пользователя, в рамках одной сессии может быть много ecid и requestid
	 * [UserName] nvarchar(40) not null		--[6] => 'username' -- string: username - логин пользователя
	 * [Info] nvarchar(255) not null		--[7] => 'General Query Info:' -- string: комментарий к записи из лога перед телом основного сообщении/запроса
	 * [Text] ntext							--[8] => Repository: Star, Subject Area: RG_CHECK, Presentation: RG_CHECK  -- string: текст запроса/сообщения записи из лога
	 * [UnixTimestamp] bigint not null		--[9] => 1589448252 bigint как UNIX TIMESTAMP (секунды) на основе [1] - добавил на всякий случай
	 */

	//Insert Query as MERGE
	$tsql= "MERGE [dbo].[BI_NQQUERY_LOG] AS [target]
	USING (VALUES (?)) AS [source] ([Id_Nqquery_Log])
	ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log]
	WHEN NOT MATCHED THEN INSERT VALUES(?,CONVERT(DATETIME,?),?,?,?,?,?,?,?,?);";

	$params = [
		$params[0], 
		$params[0], 
		$params[1], 
		$params[2], 
		$params[3], 
		$params[4], 
		$params[5], 
		$params[6], 
		$params[7],
		$params[8],
		$params[9]
	];

	$return = 'success'; //вернем если запрос успешный, иначе вернем форматированную строку ошибки от sqlsrv_errors

	$result = sqlsrv_query($connection, $tsql, $params);
	if ($result === FALSE) {
		//сюда попадем только если SQL ошибка запроса в sqlsrv_query
		//throw new Exception("Error: MSSQL SQL Query failed! ".print_r( sqlsrv_errors(), true));
		$return = FormatErrors(sqlsrv_errors());
	} else {

		$rowsAffected = sqlsrv_rows_affected($result);
		if ($rowsAffected === FALSE) {
			//сюда попадем только если ошибка запроса в sqlsrv_rows_affected()
			//throw new Exception("Error: MSSQL SQL rows affected failed! ".print_r( sqlsrv_errors(), true));
			$return = FormatErrors(sqlsrv_errors());
		} else {
			//ToDo добавить подсчет вставленных записей и вывод в итоге
			//echo ($rowsAffected. " row(s) inserted " . PHP_EOL);
			$inserted += $rowsAffected;
		}
	}
	sqlsrv_free_stmt($result);
	return $return;
}

/**
 * Format to single string sqlsrv_errors() errors
 * @param $errors Null or Array as sqlsrv_errors() result
 * @return string of formeted errors as single string without line ending
 */
function FormatErrors($errors) :string {

	if($errors === Null) {
		//сюда попадаем если нет ошибки вызова sqlsrv_* функции, поэтому sqlsrv_errors() вернула Null
		$return = 'Function sqlsrv_errors() return NULL, That is not an error. Check FormatErrors() calling';

	} elseif (is_array($errors)) {
		//сюда попадаем только если sqlsrv_errors вернула массив с ошибками
		$return = "Sqlsrv_errors information: ";
		foreach ( $errors as $error ) {
			$return .= " SQLSTATE: ".$error['SQLSTATE'];
			$return .= " Code: ".$error['code']."";
			$return .= " Message: ".$error['message']."";
		}

	} else {
		//это излишне, но параноя - сюда попадаем только если sqlsrv_errors вернула не массив ошибками
		$return .= 'Function sqlsrv_errors() return not an array, That is not an error. Check FormatErrors() calling';
	}
	return $return;
}

/**
 * Get Connect to MSSQL DB
 * @param string
 * @param array
 * @return connection or Exception with formated sqlsrv_errors as string
 */
function getConnection(string $serverName, array $connectionOptions) {
    // $serverName = 'host';
	// $connectionOptions = [
    //     'Database'   => 'DB',
    //     'CharacterSet'  => 'UTF-8',
	//     //"Uid"=>'mssqlserverusername',   //not needed for win authentication
	//     //"PWD"=>'password',  //not needed for win authentication
    // ];
    //Get Connect to DB using Windows Authentication
	$connection = sqlsrv_connect( $serverName, $connectionOptions ); //если вернет FALSE, то значит ошибка подключения!
	if( $connection === false ) {
        //FormatErrors( sqlsrv_errors() ); //вернет форматированну строку ошибки sqlsrv_errors
        //throw new Exception("Error: MSSQL connection failed!"); // краткий вариант сообщения
    	throw new Exception("Error: MSSQL connection failed! ".FormatErrors(sqlsrv_errors()) );
	}
	return $connection;
}

/**
 * Close DB conection
 * @param connection
 * @return void
 */
function closeConnection($connection) {
	sqlsrv_close($connection);
}




 


   


