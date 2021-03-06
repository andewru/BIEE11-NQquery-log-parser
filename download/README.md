BIEE11 NQQuery.log downloaded
=============================

## download-ssh.php

Для работы файла требуется PHP 7.2 и модуль PECL SSH2

Файл `download-ssh.php` выполняет скачивание по SSH с удаленного сервера файлов журналов приложения. Все настройки путей, параметров выполняются в начале файла. Логика скрипта позволяет запуск только одного instans скрипта, поэтому нужно убедиться, что предыдущий run уже завершился, иначе скрипт будет работать не верно и можно потерять данные в логе скрипта.

Скрипт запускается через планировщик, обычно раз в сутки.

`php -f C:\your-path\NQquery\download\download-ssh.php` - команда для запуска скрипта в планировщике Windows.

## Процесс обновления файлов журналов приложения

Скрипт писался под следующий процесс. На сервере BI 11 приложение постоянно пишет лог файл с именем` nqquery.log`. При достижении файлом `nqquery.log` размера 100MB, приложение создает новый файл `nqquery.log` и начинает запись уже в него, а старый лог файл переименовывает в `nqquery*.log` (* это обычно номер или дата) и больше его не изменяет. Переименованный файл храниться на сервере установленное количество дней (например 10 дней) и по истечении этого срока удаляется приложением.

## Задание

Необходимо организовать ежедневное (раз в сутки) получение данных логирования BI нарастающим итогом. Для этого необходимо организовать ежедневное (раз в сутки) скачивание с сервера BI текущего `nqquery.log` файла и старых, переименованных  `nqquery*.log` файлов, которые еще не были скачаны ранее, т.к. мы не знаем в какой момент будет создан новый переименованный файл.

### Критерий нового файла

Файл считается новым, если у него с последнего запуска скрипта `download-ssh.php` изменилось имя и/или mtime (время последней модификации как unix time) на удаленном сервере. Для этого скрипт ведет лог файл `./download-ssh-log.json` своей работы, где хранит информацию о ранее скачанных файлах с ключом в виде md5 хеша от строки `unixtimestamp-filename` где:

- unixtimestamp - int - время последней модификации (временная метка Unix) файла на удаленном сервер
- filename - str - имя файла на удаленном сервере

### Цикл работы скрипта

Скрипт `download-ssh.php` запускается через планировщика раз в сутки и читает список файлов нужной директории на удаленном сервере BI через SSH. Список файлов директории прокручивается в цикле и для имени файла соответствующего заданному регулярному выражению выполняются дальнейшие действия. Для каждого файла удовлетворяющего условию выполняется проверка по лог файлу скрипта `./download-ssh-log.json` на предмет является ли это файл новым по вышеописанным критериям. Если файл является новым, то файл скачивается в директорию `./downloaded-files` с новым именем как `unixtimestamp-filename`.

## Ошибки


Ошибки PHP и ошибки скачивания логируются скриптом в файл `./download-ssh-errors.log`

## Замечания

Скрипт самостоятельно не очищает от старых файлов директорию загрузки, свой лог и лог ошибок, поэтому они могут разрастись в количестве и размерах.

