


-- BI_EVENTS ������� ��� �������� ����������� �� ������ BI

USE [MD]
GO
-- ������� ������������
INSERT INTO [dbo].[BI_EVENTS]
           ([eventDate]    -- unique 
           ,[eventDescr])
     VALUES
		    (cast('2020-04-13' as date),N'���������� �������� � BI � �������� ������ �� ������ ����� ���������� ��������')
		   ,(cast('2020-04-14' as date),N'���������� �������� � BI � �������� ������ �� ������ ����� ���������� ��������. ��� �������� � �����')
		   ,(cast('2020-04-15' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-16' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-17' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-18' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-19' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-20' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-21' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-22' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
		   ,(cast('2020-04-23' as date),N'������ �� ������ ����� ���������� ��������, ������� ������. ����������� �������� sales_ret')
		   ,(cast('2020-04-24' as date),N'������ �� ������ ����� ���������� ��������, ������� ������')
           ,(cast('2020-04-27' as date),N'�������� ���������� DWH � BI, �������� ��� ���� � ������, ������ �� DWH, �������� �� sales_ret �������, ����������� (�����). BI ����������� �� �������.')
		   ,(cast('2020-04-28' as date),N'�������� ����� ��� sales_ret, ����������� ��� ������ �������� ���, ������ ����� 20:00 BI ���� ��������. �������� ���������� DWH � BI, �������� ��� ���� � ������, ������ �� DWH, �������� �� sales_ret �������, ����������� (�����). BI ����������� �� �������.')
		   ,(cast('2020-04-29' as date),N'������ ���� ���������� ������ ����� ����������� SALE_RET, BI ���� �������� � ������� ������. ����� �������� ����������� �� ������. ������ ������������, ������������ ������������� ������������ �����������')
		   ,(cast('2020-05-14' as date),N'�������� ���������� BI, ������������ �������� �� ���������� ��������� ����������, ������� ����� �������� ���� � ��������')
		   ,(cast('2020-05-15' as date),N'�������� ���������� BI, ������������ �������� �� ���������� ��������� ����������, ������� ����� �������� ���� � ��������')
GO
