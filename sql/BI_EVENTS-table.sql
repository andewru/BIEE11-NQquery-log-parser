-- BI_EVENTS ������� ��� �������� ����������� �� ������ BI

USE [MD]
GO
-- ������� ������������
INSERT INTO [dbo].[BI_EVENTS]
           ([eventDate]    -- unique 
           ,[eventDescr])
     VALUES
		    (cast('2018-04-13' as date),N'���������� �������� � BI � �������� ������ �� ������ ����� ���������� ��������')
		   ,(cast('2018-04-14' as date),N'���������� �������� � BI � �������� ������ �� ������ ����� ���������� ��������. ��� �������� � �����')
GO
