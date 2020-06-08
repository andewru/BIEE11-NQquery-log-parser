<?php
/**
 * Не должно быть одновременно и параллельно работающих нескольких instans скрипта, т.к. логика его работы это не предусматривает.
 * Перед каждым запуском нужно убедиться, что скрипт уже не работает.
 * Скачивает только новые файлы. Файл считается новым, если у него изменилось с последнего запуска скрипта
 * имя или mtime (время последней модификации как unix time) на удаленном сервере.
 * Скачанный файл переименовывается добавлением к нему префикса с его mtime: 1591282723-nqquery.log
 * Тестировался на файлах с именами на латинице и без пробелов!
 * Информация о скачанных файлах храниться в Log файле скрипта. ToDo доделать чистилку лога от старых данных.
 */

/**
 * Скачивание по SSH файлов с удаленного сервера с авторизацией по логину и паролю
 * 
 * 
 * Отбирает файлы для скачивания по паттерну и скачивает в указанную директорию
 * ToDo - доделать, что бы качал только новые файлы, а ранее скачанные исключал
 * 
 * @require PECL SSH2 https://www.php.net/manual/ru/ref.ssh2.php
 */
 /**
  * SSH connect option
  */
$host = 'your-host';
$port = 22;
$username = 'username';
$password = 'password';
/**
 * Директория файлов на удаленном сервере, абсолютный путь с конечным слэшем
 * /oracle/Middleware/instances/instance1/diagnostics/logs/OracleBIServerComponent/coreapplication_obis1/nqquery*.log - искомые файлы
 * @var string
 */
$rDir = '/oracle/Middleware/instances/instance1/diagnostics/logs/OracleBIServerComponent/coreapplication_obis1/';
/**
 * Pattern имени файла для поиска в $rDir
 * nqquery*.log - искомые файлы
 * @var string
 */
$pattern = "/^nqquery[-0-9a-z]{0,10}\.log$/siU";
/**
 * Директория загрузики файлов, абсолютный путь с конечным слэшем
 * @var string
 */
$lDir =  str_replace("\\", "/", __DIR__.'/downloaded-files/');

/**
 * Log файл и одновременно Data file для управления работой download-ssh.php.
 * Решил основываться на Log файле, а не на списке файлов директории загрузки,
 * т.к. в ней файлы могут быть удалены или модифицированными сторонними скриптами.
 * 
 * Хранит информацию о запусках download-ssh.php, скачанных файлах, ошибках и
 * служит для управления процессом скачивания, что бы исключить
 * повторные скачивания файлов, если они не модифицировались на удаленном сервере.
 * Критерий проверки для скачивания - имя удаленного файла и его mtime (время последней модификации),
 * что бы всегда скачивать только новые файлы, не скачанные еще ранее.
 * Log файл полностью управляется скриптом, поэтому его не нужно модифицировать руками, можно только читать.
 * Если Log файл отсутствует, то будет создан новый при запуске скрипта.
 * @var string
 */
$logFile = str_replace("\\", "/", __DIR__.'/download-ssh-log.json');

/**
 * Файл логирования ошибок работы скрипта
 * @var string
 */
$errorLog = str_replace("\\", "/", __DIR__.'/download-ssh-errors.log');


// ---------------------------------------------------------------------------------------------------------------------------
try {

  //включение логирования ошибок PHP в заданный файл, в него сообщения пишутся нарастающим итогом
  //error_reporting(E_ALL);
  ini_set('error_log', $errorLog ); //задаем файл в который будем писать, если файла нет, то он будет создан автоматически
  ini_set('log_errors', true); //включаем логирования ошибок PHP

  $startTime = microtime(true); //microtime as float value of seconds
  $time = time(); //временная метка Unix для логирования старта скрипта

  $connection = ssh2_connect($host, $port);
  ssh2_auth_password($connection, $username, $password);

  $sftp = ssh2_sftp($connection);
  $dh = opendir("ssh2.sftp://$sftp$rDir"); //вернет resource, или FALSE если ошибка
  if( $dh  === false ) {
    throw new Exception("Error: opendir() failed!");
  }

  //чтение массива из лога скрипта
  $log = getLog($logFile);

  //Чтение файлов директории $dh, в $file имя файла будет - это могут быть как файлы так и директории!
  while (($file = readdir($dh)) !== false ) {

    //отбираем только нужные файлы
    //ToDo протестировать проверку filetype("ssh2.sftp://$rDir.$file") === 'file' на файл, пока не стал т.к. это удаленная директория, возможно filetype() тут будет не уместна.
    //if(@preg_match($pattern , $file) === 1 && @filetype("ssh2.sftp://$rDir.$file") === 'file' )
    if(@preg_match($pattern , $file) === 1 ) {

      $statinfo = ssh2_sftp_stat($sftp, $rDir.$file); //array инфы по файлу
      $mtime = $statinfo['mtime']; //время последней модификации (временная метка Unix)
      $mDate = date('c', $statinfo['mtime']); //время последней модификации (временная метка Unix) - конверт в 2020-06-01T07:48:44+00:00 по UTC
      //$atime = $statinfo['atime'];                //время последнего доступа (временная метка Unix)
      $size = round($statinfo['size']/1024/1024) ;  //размер в байтах, и перевов в МБ = байты/1024/1024 - 38
      //ключ файла как unixtimestamp-filename
      $fileIndex = $mtime.'-'. $file;
      //ключ файла в логе: как md5 хеш от unixtimestamp-filename, т.к. имя файла может быть с пробелами и т.п., поэтому для ключа массива делаем хеш
      $logIndex = md5($fileIndex);
      $logIndexFail = $logIndex.'-fail'; //ключ для ошибки загрузки для записи в лог

      if(array_key_exists($logIndex, $log) === false) {
        //качаем если новый файл
        echo 'Found new file: '. $file.', size: '.$size.'MB', ', Modified date: '.$mDate.PHP_EOL;
        echo 'Start downloading to '.$lDir.$fileIndex.PHP_EOL.' ... '.PHP_EOL;

        //готовим данные для лога
        $log[$logIndex] = [
            'name' => $file
            ,'mtime' => $mtime
            ,'mDate' => $mDate
            ,'size' => $statinfo['size']
            ,'logTime' => $time
            ,'logDate' => date('c', $time)
            ,'download' => 'start' //статус начала загрузки
        ];

        //Копирование файла с сервера на клиент, используя протокол SCP
        if(ssh2_scp_recv($connection, $rDir.$file, $lDir.$fileIndex) === true ) {
          //Возвращает TRUE в случае успешного завершения или FALSE в случае возникновения ошибки
          echo 'Download successful to '.$lDir.$fileIndex.PHP_EOL;
          $log[$logIndex]['download'] = 'successful'; //статус успешной загрузки
        } else {
          echo 'Download failed for '. $file.', size: '.$size.'MB', ', mtime: '.$mDate.PHP_EOL;
          $log[$logIndex]['download'] = 'failed'; //статус неудачной загрузки
          $log[$logIndexFail] = $log[$logIndex]; //сохраняем в лог как ошибочную загрузку
          unset($log[$logIndex]); //удаляем лог для успешной загрузки, т.к. сохраним его с ключом для ошибки загрузки

          error_log('Download failed for '. $file.', size: '.$size.'MB', ', mtime: '.$mDate); //пишем в заданный выше PHP error_log свое сообщение
        }
      }
    }
  }

  closedir($dh);
  //ssh2_disconnect ($connection); //дает почему то Segmentation fault сообщение, хотя все отрабатывает нормально, поэтому отключил, соединение и так закроется.

  //перезапись лога
  putLog($logFile, $log);

  $endTime = microtime(true);
  $executeTime = $endTime - $startTime;
  echo 'Script execute time: '.round($executeTime, 3).' seconds.'.PHP_EOL;


} catch (\Exception $e) {
  echo $e->getMessage();
  error_log($e->getMessage()); // добавление в конец лог файл записи об ошибки для исключений. Не обязательно, т.к. любая PHP ошибка и так будет залогированна.
  exit();
}


// ---------------------------------------------------------------------------------------------------

//ToDo дописать функцию удаления старых записей из лога (что бы он не разросся) и файлов...

/**
 * Получить Log массив из log файла
 * 
 * @param string as path
 * @return array
 */
function getLog(string $logFile) : array {

  if (file_exists($logFile) && is_readable ($logFile)) {

    $data = file_get_contents($logFile); // файл как строку в переменную
    $log = json_decode($data, TRUE); // Декодировать в массив

    if(is_array($log) && count($log) > 0)
      return $log;
  }

  $log = [];
  return $log;
}

/**
 * Перезаписывает лог файл свежими данными
 * 
 * @param string
 * @param array
 */
function putLog(string $logFile, array $log) : void {

  file_put_contents($logFile, json_encode($log, JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_QUOT | JSON_HEX_AMP | JSON_UNESCAPED_UNICODE | JSON_FORCE_OBJECT | JSON_PRETTY_PRINT)); // Перекодировать в формат json и записать в файл.

}


