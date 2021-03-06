/****** 
-- [Id_Nqquery_Log]
--,[LogDateTime]
--,[User]
--,[ExecuteId]
--,[RequestId]
--,[SessionId]
--,[UserName]
--,[Info]
--,[Text]
--,[UnixTimestamp]

--[User] -- как код залогированного действия и [Info] для данного значения:
--[User] = 0  -- START - SQL Request, logical request hash:   -- начало запроса! --------------------------
--[User] = 2  -- Logical Request (before navigation):
--[User] = 7  -- Cache Diagnostic Information, Pass 1:
--[User] = 16 -- Execution plan:
--[User] = 18 -- Sending query to database  -- запрос отправленные в базу данных
--[User] = 20 -- Execution Node: <<31538331>>, Close Row Count = 30327, Row Width = 648 bytes
--[User] = 23 -- General Query Info:  -- общая информация по запросу - в тексте будет тогда типа - Repository: Star, Subject Area: RG_CHECK, Presentation: RG_CHECK
--[User] = 24 -- Rows returned to Client -- количество строк вернутых запросом из базы
--[User] = 26 -- Rows 30327, bytes 19651896 retrieved from database query id: <<31538331>>
--[User] = 28 -- Physical query response time 37 (seconds)  ------------- время запроса к базе данных
--[User] = 29 -- Physical Query Summary Stats: Number of physical queries 1, Cumulative time 37, DB-connect time 0 (seconds) -- обшая информация по запросе к базе данных! и время в секундах - БЕРИ ОТ СЮДА ИТОГ ПО ЗАПРОСУ! --------------
--[User] = 33 -- Logical Query Summary Stats: Elapsed time 15, Response time 14, Compilation time 0 (seconds) --------------------
--[User] = 34 -- Query Status:   -- Query Status: Successful Completion ИЛИ ошибка!, например: Query Status: [nQSError: 17012] Bulk fetch failed.  -----------------
--[User] = 36 -- Cancel initiated for Exchange Producer: <<31503256>> DbGateway Exchange: DB_RG_SBL.rgCP
--[User] = 37 -- Query execution terminated: <<31503256>>
--[User] = 39 -- An initialization block named 'INIT_VAR_load_date', on behalf of a Session Variable, issued the following SQL query:
--[User] = 48 -- The logical query block fail to hits or seed the cache in subrequest level due to
--[User] = 50 -- The logical query [hits the plan cache | disqualifies the plan cache]
******/
USE [MD]
GO
-- TRUNCATE TABLE [dbo].[BI_NQQUERY_LOG];
SELECT COUNT(*) FROM [MD].[dbo].[BI_NQQUERY_LOG]; -- 26-05-20 143398 записей

SELECT  COUNT(*) FROM [MD].[dbo].[BI_NQQUERY_LOG] WHERE [User] = 0; -- 12320 всего запросов в таблице 7011 успешных 360 иных -- -- distinct [RequestId]  24143
SELECT  * FROM [MD].[dbo].[BI_NQQUERY_LOG] 
WHERE [User] = 34 and [Info] LIKE '%Query Status: Successful Completion%';

-- ГРУППИРОВКА из DATETIME по дням DATE с подсчетом количества запросов в каждый день ----------------------------------------------------------------------------
SELECT
 Cast([LogDateTime] as Date) [Date]
,COUNT(*) [Count]
FROM [MD].[dbo].[BI_NQQUERY_LOG]
WHERE [User] = 0
GROUP BY Cast([LogDateTime] as Date) 
ORDER BY [Date];
------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Для статистики - сводная информация по запросам через их START - SQL Request  -- 7307 строк
SELECT DISTINCT
CAST([T].[LogDateTime] AS DATE) AS [Date]
,[T].[ExecuteId]
,[T].[RequestId]
,[T].[SessionId]
,[T].[UserName]
,CASE 
	WHEN 
		(SELECT 1 FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [SS] 
		WHERE ([SS].[RequestId] = [T].[RequestId] AND [SS].[ExecuteId] = [T].[ExecuteId] AND [SS].[SessionId] = [T].[SessionId] AND [SS].[UserName] = [T].[UserName])
		AND [SS].[User] = 34 -- Query Status:
		AND [SS].[Info] LIKE '%Query Status: Successful Completion%'
	) = 1 THEN 'success'
	WHEN (SELECT 1 FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [SS] 
		WHERE ([SS].[RequestId] = [T].[RequestId] AND [SS].[ExecuteId] = [T].[ExecuteId] AND [SS].[SessionId] = [T].[SessionId] AND [SS].[UserName] = [T].[UserName])
		AND [SS].[User] = 34 -- Query Status:
		AND [SS].[Info] LIKE '%error%'
	) = 1 THEN 'error'
	ELSE (SELECT [SS].[Info] FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [SS] 
		WHERE ([SS].[RequestId] = [T].[RequestId] AND [SS].[ExecuteId] = [T].[ExecuteId] AND [SS].[SessionId] = [T].[SessionId] AND [SS].[UserName] = [T].[UserName])
		AND [SS].[User] = 34 -- Query Status:
	)
END AS [QueryStatus]

,(SELECT  -- [SS].[Info] -- Physical Query Summary Stats: Number of physical queries 1, Cumulative time 230, DB-connect time 0 (seconds)
	SUBSTRING(
				SUBSTRING([SS].[Info], Patindex('%Cumulative time [0-9]%', [SS].[Info]) + 16, LEN([SS].[Info]) - Patindex('%Cumulative time [0-9]%', [SS].[Info]) + 16)
				,0
				,Charindex(',', SUBSTRING([SS].[Info], Patindex('%Cumulative time [0-9]%', [SS].[Info]) + 16, LEN([SS].[Info]) - Patindex('%Cumulative time [0-9]%', [SS].[Info]) + 16))
	)
		FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [SS] 
		WHERE [SS].[User] = 29 -- Physical Query Summary Stats
		and ([SS].[RequestId] = [T].[RequestId] AND [SS].[ExecuteId] = [T].[ExecuteId] AND [SS].[SessionId] = [T].[SessionId] AND [SS].[UserName] = [T].[UserName])	 
) AS [ResponseTime]

,(SELECT  [SS].[Info] -- Physical Query Summary Stats: Number of physical queries 1, Cumulative time 230, DB-connect time 0 (seconds)
		FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [SS] 
		WHERE [SS].[User] = 29 -- Physical Query Summary Stats
		and ([SS].[RequestId] = [T].[RequestId] AND [SS].[ExecuteId] = [T].[ExecuteId] AND [SS].[SessionId] = [T].[SessionId] AND [SS].[UserName] = [T].[UserName])	 
) AS [QuerySummary]

FROM [MD].[dbo].[BI_NQQUERY_LOG] AS [T]
WHERE [T].[UserName] != 'BISystemUser' AND [T].[UserName] != 'weblogic'
AND [T].[User] = 0  -- 0 это начало запроса
;
-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------





  

