USE [MD]
GO
-- � WEB ����� ������ ����� ������������ ��� 23.05.2020 10:15:23 GMT+04:00 ��� ������� ���� ����������, �� ����� +1 ��� � ��������� �������, � � ���� ��� ���� ���
-- [1] => 2020-05-23T09:15:18.000+03:00 -- � ���� ��� �������� ����� ������� �� MSK, ���� ���� ����� +3 - ������� ����� ����� ��� ����� ��� �������� ����� ������� �� MSK - ��������!
-- �.�. ��� MSSQL � MSK ����, �� � �� ������� �� ���� [2020-05-23T09:15:18.000+03:00] ���� ����� ������ ��� �������� ����� � �������� �� ��� DATETIME, ��� � ����� � ���� ������ �������� ����� ������� �� MSK
select CONVERT(DATETIME, '2020-05-23T09:15:18.000');--2020-05-23 09:15:18.000
USE [MD]
GO
-- drop table [BI_NQQUERY_LOG];
USE [MD]
GO
-- BI_NQQUERY_LOG] - ������� ��� �������� ����������� �������� NQQuery.log BI, ������� �������� ������� ������������� BI
CREATE TABLE [BI_NQQUERY_LOG] (

 [Id_Nqquery_Log] nchar(32) not null			--[0] => '25ebd9a574ffbcc08e7da214e55397ef' string: md5 hash ������ �� ����, ��� ���������� ���������� ��� ��������� �������� ��� �� ������
,[LogDateTime] datetime not null				--[1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
,[User] tinyint not null						--[2] => 23 int: ��� user ��� ������ �� ���� �� USER-23 - ���� ��� ����� ������������ ��� ���� ���� ������
,[ExecuteId] nvarchar(60) not null				--[3] => 'b5825c03a3ed7752:64b3e9bc:16ea4849159:-8000-00000000939da4c0' string ��� 'ecid' ��� ������ �� ���� - execute id, �������� (� ������ ����� ������ �����), � ������ ������ ecid ����� ���� ��������� requestid
,[RequestId] nchar(8) not null					--[4] => '632f0010' string: requestid - ���������� ������������� ������� BI ��� ������ �� ����, �������������� ���� ������� ����������� � ������ ������� � ������ ������ ecid
,[SessionId] nchar(8) not null					--[5] => '632f0000' string: sessionid - ������������� ������ ������������, � ������ ����� ������ ����� ���� ����� ecid � requestid
,[UserName] nvarchar(40) not null				--[6] => 'username' -- string: username - ����� ������������
,[Info] nvarchar(255) not null					--[7] => 'General Query Info:' -- string: ����������� � ������ �� ���� ����� ����� ��������� ���������/�������
,[Text] ntext									--[8] => Repository: Star, Subject Area: RG_CHECK, Presentation: RG_CHECK  -- string: ����� �������/��������� ������ �� ����
,[UnixTimestamp] bigint not null				--[9] => 1589448252 bigint ��� UNIX TIMESTAMP (�������) �� ������ [1] - ������� �� ������ ������

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
--TRUNCATE TABLE [dbo].[BI_NQQUERY_LOG_TEST]; -- ������������ ��� ������������ �������, ��� �� �� ������ ������ � �������� �������
USE [MD]
GO

-- INSERT INTO [dbo].[BI_NQQUERY_LOG_TEST] select * from [dbo].[BI_NQQUERY_LOG] -- ������� ������ ���� ���� ����� ������ �������� ������� �� ����������, ��� �� ���� ��� ���������
     
GO









-- MERGE----------------------------------------------------------------------------------
-- https://docs.microsoft.com/ru-ru/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver15&viewFallbackFrom=sql-server-ver17
USE [MD]
GO
-- ---------------------------------------------------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- �������, ������� ����� ��������
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
		)

     ) AS [source] 
		(
			[Id_Nqquery_Log] -- PRIMARY KEY
		)

-- �������� ��� �������� ������ �������
ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- ������� ����� ����� ���������� � ���������� ��������

WHEN NOT MATCHED THEN


    INSERT -- ��� ��������
		 
	VALUES(
	
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] =>
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
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


-- �������� �������, �� ���� ��������� ----------------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- �������, ������� ����� ��������
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
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
		)  -- �������� ������, ������� �� ����������� ����


ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- ������� ����� ����� ���������� � ���������� ��������

WHEN NOT MATCHED THEN


    INSERT -- ��� ��������
		 
	VALUES(
	
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] =>
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
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



-- ����� ������ ������� - ��������� ----------------------------------------------
MERGE [dbo].[BI_NQQUERY_LOG] AS [target]  -- �������, ������� ����� ��������
USING (VALUES 

		(
			'25ebd9a574ffbcc08e7da214e55397ef'				-- [0] => PRIMARY KEY
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
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
		)  -- �������� ������, ������� �� ����������� ����


ON [target].[Id_Nqquery_Log] = [source].[Id_Nqquery_Log] -- ������� ����� ����� ���������� � ���������� ��������

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
           ,CONVERT(DATETIME, '2020-05-23T09:15:18.000')	-- [1] => '2020-05-23T09:15:18.000' ��� CONVERT(DATETIME, [1]) 
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



