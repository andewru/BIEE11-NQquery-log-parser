USE [MD]
GO
-- в WEB админ панели время показывается как 23.05.2020 10:15:23 GMT+04:00 где часовой пояс правильный, но время +1 час к реальному запуску, а в логе уже идет как
-- [1] => 2020-05-23T09:15:18.000+03:00 -- в логе уже реальное время запуска по MSK, хотя пояс стоит +3 - поэтому нужно брать это время как реальное время запуска по MSK - проверил!
-- т.к. мой MSSQL в MSK зоне, то я от времени из лога [2020-05-23T09:15:18.000+03:00] беру часть строки без часового пояса и сохраняю ее как DATETIME, это и будет в моем случае реальное время запуска по MSK
select CONVERT(DATETIME, '2020-05-23T09:15:18.000');--2020-05-23 09:15:18.000
USE [MD]
GO
-- drop table [BI_NQQUERY_LOG];
USE [MD]
GO
-- BI_NQQUERY_LOG] - таблица для хранения результатов парсинга NQQuery.log BI, который содержит запросы пользователей BI
CREATE TABLE [BI_NQQUERY_LOG] (

 [Id_Nqquery_Log] nchar(32) not null			--[0] => '25ebd9a574ffbcc08e7da214e55397ef' string: md5 hash записи из лога, для вычисления дубликатов при повторном парсинге тех же файлов
,[LogDateTime] datetime not null				--[1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
,[User] tinyint not null						--[2] => 23 int: код user для записи из лога от USER-23 - этот код можно использовать как флаг типа записи
,[ExecuteId] nvarchar(60) not null				--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0' string код 'ecid' для записи из лога - execute id, уникален (в рамках одной сессии точно), в рамках одного ecid может быть несколько requestid
,[RequestId] nchar(8) not null					--[4] => '632f0010' string: requestid - уникальный идентификатор запроса BI для записи из лога, идентифицирует блок записей относящихся к одному запросу в рамках одного ecid
,[SessionId] nchar(8) not null					--[5] => '632f0000' string: sessionid - идентификатор сессии пользователя, в рамках одной сессии может быть много ecid и requestid
,[UserName] nvarchar(40) not null				--[6] => 'username' -- string: username - логин пользователя
,[Info] nvarchar(255) not null					--[7] => 'General Query Info:' -- string: комментарий к записи из лога перед телом основного сообщении/запроса
,[Text] ntext									--[8] => Repository: Star, Subject Area: RG_CHECK, Presentation: RG_CHECK  -- string: текст запроса/сообщения записи из лога
,[UnixTimestamp] bigint not null				--[9] => 1589448252 bigint как UNIX TIMESTAMP (секунды) на основе [1] - добавил на всякий случай

,CONSTRAINT [PK_Id_Nqquery_Log] PRIMARY KEY NONCLUSTERED (Id_Nqquery_Log)
,INDEX [Ci_LogDateTime] CLUSTERED (LogDateTime DESC)
,INDEX [Ni_User] NONCLUSTERED ([User])
,INDEX [Ni_ExecuteId] NONCLUSTERED ([ExecuteId])
,INDEX [Ni_RequestId] NONCLUSTERED ([RequestId])
,INDEX [Ni_SessionId] NONCLUSTERED ([SessionId])
,INDEX [Ni_UserName] NONCLUSTERED ([UserName])
)

-- ---------------------------------------------------------------------------------------
USE [MD]
GO
--TRUNCATE TABLE [dbo].[BI_NQQUERY_LOG];
--TRUNCATE TABLE [dbo].[BI_NQQUERY_LOG_TEST]; -- использовала для тестирования парсера, что бы не убдить данные в основной таблице
USE [MD]
GO

-- INSERT INTO [dbo].[BI_NQQUERY_LOG_TEST] select * from [dbo].[BI_NQQUERY_LOG] -- перелил данные пока сюда перед первым запуском парсара по расписанию, что бы если что откатится
     
GO









-- MERGE----------------------------------------------------------------------------------
-- https://docs.microsoft.com/ru-ru/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver17
USE [MD]
GO
-- ---------------------------------------------------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- таблица, которая будет меняться
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
		)

     ) AS [source] 
		(
			[Id_Nqquery_Log] -- PRIMARY KEY
		)

-- источник как значения вместо таблицы
ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- условие связи между источником и изменяемой таблицей

WHEN NOT MATCHED THEN


    INSERT -- все столбыцы
		 
	VALUES(
	
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] =>
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
           ,23												--[2] => 23
           ,'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'
           ,'632f0010'		--[4] => '632f0010' string: requestid 
           ,'632f0000' --[5] => '632f0000' string: sessionid
           ,'username'  --[6] => 'username' 
           ,'General Query Info:' --[7] => 'General Query Info:'
           ,'Repository: Star' -- --[8] => Repository: Star
           ,1589448252 --[9] => 1589448252
		)

;	


-- покороче вариант, но тоже избыточно ----------------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- таблица, которая будет меняться
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
           ,23												--[2] => 23
           ,'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'
           ,'632f0010'		--[4] => '632f0010' string: requestid 
           ,'632f0000' --[5] => '632f0000' string: sessionid
           ,'username'  --[6] => 'username' 
           ,'General Query Info:' --[7] => 'General Query Info:'
           ,'Repository: Star' -- --[8] => Repository: Star
           ,1589448252 --[9] => 1589448252
		)

     ) AS [source] 
		(
			[Id_Nqquery_Log] -- PRIMARY KEY
           ,[LogDateTime]
           ,[User]
           ,[ExecuteId]
           ,[RequestId]
           ,[SessionId]
           ,[UserName]
           ,[Info]
           ,[Text]
           ,[UnixTimestamp]
		)  -- источник данных, который мы рассмотрели выше


ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- условие связи между источником и изменяемой таблицей

WHEN NOT MATCHED THEN


    INSERT -- все столбыцы
		 
	VALUES(
	
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] =>
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
           ,23												--[2] => 23
           ,'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'
           ,'632f0010'		--[4] => '632f0010' string: requestid 
           ,'632f0000' --[5] => '632f0000' string: sessionid
           ,'username'  --[6] => 'username' 
           ,'General Query Info:' --[7] => 'General Query Info:'
           ,'Repository: Star' -- --[8] => Repository: Star
           ,1589448252 --[9] => 1589448252
		)

;	



-- самый полный вариант - избыточно ----------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- таблица, которая будет меняться
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
           ,23												--[2] => 23
           ,'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'
           ,'632f0010'		--[4] => '632f0010' string: requestid 
           ,'632f0000' --[5] => '632f0000' string: sessionid
           ,'username'  --[6] => 'username' 
           ,'General Query Info:' --[7] => 'General Query Info:'
           ,'Repository: Star' -- --[8] => Repository: Star
           ,1589448252 --[9] => 1589448252
		)

     ) AS [source] 
		(
			[Id_Nqquery_Log] -- PRIMARY KEY
           ,[LogDateTime]
           ,[User]
           ,[ExecuteId]
           ,[RequestId]
           ,[SessionId]
           ,[UserName]
           ,[Info]
           ,[Text]
           ,[UnixTimestamp]
		)  -- источник данных, который мы рассмотрели выше


ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- условие связи между источником и изменяемой таблицей

WHEN NOT MATCHED THEN


    INSERT 
		 (
		
			[Id_Nqquery_Log] -- PRIMARY KEY
           ,[LogDateTime]
           ,[User]
           ,[ExecuteId]
           ,[RequestId]
           ,[SessionId]
           ,[UserName]
           ,[Info]
           ,[Text]
           ,[UnixTimestamp]
		
		)
	
	
	VALUES(
	
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] =>
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' как CONVERT(DATETIME, [1]) 
           ,23												--[2] => 23
           ,'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'	--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0'
           ,'632f0010'		--[4] => '632f0010' string: requestid 
           ,'632f0000' --[5] => '632f0000' string: sessionid
           ,'username'  --[6] => 'username' 
           ,'General Query Info:' --[7] => 'General Query Info:'
           ,'Repository: Star' -- --[8] => Repository: Star
           ,1589448252 --[9] => 1589448252
		)

;	



