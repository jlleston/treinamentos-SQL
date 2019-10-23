-----------------------------------------------
------[ MODULO 2 - Novas Instruções T-SQL]-----
-----------------------------------------------

--> Função STRING_AGG()
-------------------------

	-- Resultado antes da STRING_AGG
	select
			s.StockItemName		as Produto,
			sg.StockGroupName	as Grupos
	
	from [WAREHOUSE].stockitems s
	inner join [WAREHOUSE].stockitemstockgroups sisg
		on s.StockItemID = sisg.StockItemID
	inner join [WAREHOUSE].stockgroups sg
		on sisg.StockGroupID = sg.StockGroupID
	order by Produto, Grupos;
	GO

	-- Com a STRING_AGG()
	select
			s.StockItemName							as Produto,
			STRING_AGG(sg.StockGroupName,' , ')		as Grupos
	
	from [WAREHOUSE].stockitems s
	inner join [WAREHOUSE].stockitemstockgroups sisg
		on s.StockItemID = sisg.StockItemID
	inner join [WAREHOUSE].stockgroups sg
		on sisg.StockGroupID = sg.StockGroupID
	group by s.StockItemName
	order by Produto, Grupos;
	GO

	-- Com a STRING_AGG() e ordenação no agrupamento
	select
			s.StockItemName									as Produto,
			STRING_AGG(sg.StockGroupName,' , ')
				WITHIN GROUP (ORDER BY sg.StockGroupName)	as Grupos
	
	from [WAREHOUSE].stockitems s
	inner join [WAREHOUSE].stockitemstockgroups sisg
		on s.StockItemID = sisg.StockItemID
	inner join [WAREHOUSE].stockgroups sg
		on sisg.StockGroupID = sg.StockGroupID
	group by s.StockItemName
	order by Produto;
	GO

--> Função STRING_SPLIT(), TRANSLATE() e TRIM()
-----------------------------------------------

	declare @cor varchar(100);
	
	set @cor = 'amarelo,azul,branco,cinza,marron,preto,rosa,verde,vermelho';

	select ss.[value] as Cores
	from STRING_SPLIT(@cor, ',') as ss
	order by Cores desc;
	GO

	-- Exibindo o registro sem STRING_SPLIT()
	select s.StockItemName, s.Brand, c.ColorName, s.Tags
	from [WAREHOUSE].stockitems s
	inner join [PURCHASING].Suppliers sp
		on s.SupplierID = sp.SupplierID
	inner join [WAREHOUSE].Colors c
		on s.ColorID = c.ColorID
	where sp.SupplierName = 'Northwind Electric Cars';
	GO

	-- usando STRING_SPLIT(), TRANSLATE() e TRIM()
	select
			s.StockItemName,
			c.ColorName,
			s.Tags, ss.[value] as TagSemFormatacao,
			TRANSLATE(ss.[value], '[]"', '   ') as TagComEspacos,
			TRIM(TRANSLATE(ss.[value], '[]"', '   ')) as Tag

	from [WAREHOUSE].stockitems s
	inner join [PURCHASING].Suppliers sp
		on s.SupplierID = sp.SupplierID
	inner join [WAREHOUSE].Colors c
		on s.ColorID = c.ColorID
	cross apply STRING_SPLIT(s.Tags, ',')  ss
	where sp.SupplierName = 'Northwind Electric Cars';
	GO
	

--> Função TRANSLATE() e TRIM()
--------------------------------

	select 
				TRANSLATE('{"Texto 1"},{"Texto 2"}','{}"','   ') ,
		REPLACE(TRANSLATE('{"Texto 1"},{"Texto 2"}','{}"','   ') , ' ','');
	GO

	select
			STRING_AGG(
						TRIM(
								TRANSLATE(
											ss.value, '{}"', '   '
										 )
							), ','
					  )

	from STRING_SPLIT('{"Texto 1"},{"Texto 2"}', ',') as ss
	GO

	declare @texto varchar(30) = '{"Texto 1"},{"Texto 2"}';

	select
		REPLACE(TRANSLATE(@texto,'{}"','~~~'), '~', '') as TextoLimpo,
		''
	GO

	select
		TRANSLATE('{"Texto 1"},{"Texto 2"}','{}', '()'),
		REPLACE(REPLACE('{"Texto 1"},{"Texto 2"}','{', '('), '}', ')')
	GO

	select
		TRANSLATE('abcdef','abc','bcd') as ComTranslate,
		REPLACE(REPLACE(REPLACE('abcdef','a','b'),'b','c'),'c','d') as ComReplace;
	GO

	select
		'[' + TRIM(               '  , #   com sujeira   .') + ']' ,
		'[' + TRIM( '?.#, ' from  '  , #   sem sujeira   .') + ']'
		;
	GO

--> Função CONCAT_WS()

	select 
		CONCAT('André', ',', 'Maria', ',', 'João', ',', 'Felipe e Ana'),
		CONCAT_WS(',', 'André', 'Maria', 'João', 'Felipe e Ana'),
		CONCAT_WS(', ', 'André', 'Maria', 'João', 'Felipe e Ana');
	GO


	
--> Nova sintaxe de SELECT...INTO
----------------------------------

	-- Verifica qual o FILEGROUP DEFAULT para dados (ROWS_FILEGROUP)
	select [name] from sys.filegroups where is_default = 1 and type = 'FG';
	GO

	-- Forma anterior de criar tabelas a partir de uma massa de dados (STAGEs)
	-- Relação de pedidos para entregar
	create schema Relatorios;
	GO

	select
		i.InvoiceID,
		i.OrderID,
		i.InvoiceDate,
		c.CustomerName,
		i.DeliveryInstructions,
		il.Description,
		il.Quantity,
		il.UnitPrice,
		il.TaxAmount,
		SYSDATETIME() as DataAnalise

	into [RELATORIOS].PedidosEntregar

	from SALES.Invoices i
	inner join SALES.InvoiceLines il
		on i.InvoiceID = il.InvoiceID
	inner join SALES.Customers c
		on i.CustomerID = c.CustomerID
	where
			i.ConfirmedDeliveryTime is null;

	GO

	select * from [RELATORIOS].PedidosEntregar;
	GO

	-- Verificando o FILEGROUP da tabela [RELATORIOS].PedidosEntregar
	select f.name as [Filegroup]
	from sys.indexes i
	inner join sys.filegroups f on i.data_space_id = f.data_space_id
	inner join sys.objects o on i.object_id = o.object_id
	inner join sys.schemas sc on o.schema_id = sc.schema_id
	where o.name = 'PedidosEntregar' and sc.name = 'Relatorios';
	GO

	-- Vamos usar um novo FILEGROUP

	-- Criando um novo FILEGROUP
	alter database [WideWorldImporters] add filegroup [TEMPDATA];
	GO
	alter database [WideWorldImporters]
		add file ( name = N'WWI_TempData', filename = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WideWorldImporters_TempData.ndf',
						size = 8192KB , filegrowth = 65536KB
				 ) to filegroup [TEMPDATA]
	GO

	-- Apagando a tabela criado no DEFAULT FILEGROUP
	drop table if exists [RELATORIOS].PedidosEntregar;
	GO

	-- Gerando a tabela agora em um FILEGRUP especifico
	SELECT
		i.InvoiceID,
		i.OrderID,
		i.InvoiceDate,
		c.CustomerName,
		i.DeliveryInstructions,
		il.Description,
		il.Quantity,
		il.UnitPrice,
		il.TaxAmount,
		sysdatetime() as DataAnalise

	INTO [RELATORIOS].PedidosEntregar ON [TEMPDATA]

	from SALES.Invoices i
	inner join SALES.InvoiceLines il
		on i.InvoiceID = il.InvoiceID
	inner join SALES.Customers c
		on i.CustomerID = c.CustomerID
	where
			i.ConfirmedDeliveryTime is null;

	GO

	-- Verificando novamente o FILEGROUP da tabela [RELATORIOS].PedidosEntregar
	select f.name as [Filegroup]
	from sys.indexes i
	inner join sys.filegroups f on i.data_space_id = f.data_space_id
	inner join sys.objects o on i.object_id = o.object_id
	inner join sys.schemas sc on o.schema_id = sc.schema_id
	where o.name = 'PedidosEntregar' and sc.name = 'Relatorios';
	GO


--> sp_execute_external_script - PYTHON scripts

	--> Gera uma sequencia de números randômicos
	declare @python nvarchar(max);
	declare @nl nchar(2) = ( CHAR(13) + CHAR(10) ); -- códigos ASCII para uma nova linha

	set @python =	N'import numpy as np' + @nl +
					N'import pandas as pd' + @nl +
					
					N'random_array = np.array(np.random.randint(Start,End+1,Size))' + @nl +

					N'pandas_dataframe = pd.DataFrame({"Random Numbers": random_array})'

	exec sp_execute_external_script
		@language = N'Python',
		@script = @python,
		@output_data_1_name = N'pandas_dataframe',
		@params = N'@Start INT, @End INT, @Size INT',
		@Start = 1,
		@End = 1000,
		@Size = 10
		with result sets (("Números Randômicos" int));
	GO

	/*  --> Se estiver desabilitado a execução de scripts externos
	EXEC sp_configure 'external scripts enabled', 1;
	RECONFIGURE WITH OVERRIDE
	*/

	drop table if exists #DiaSemana;
	create table #DiaSemana (id int identity, dia nvarchar(7) not null );

	insert into #DiaSemana values ('segunda'), ('terca'), ('quarta'), ('quinta'), ('sexta'), ('sabado'), ('domingo');
	insert into #DiaSemana values ('terca'), ('sexta'), ('sabado');

	EXEC sp_execute_external_script 
@language = N'Python',
@script = N'

OutputDataSet = InputDataSet

global daysMap

daysMap = {
       "segunda" : 1,
       "terca" : 2,
       "quarta" : 3,
       "quinta" : 4,
       "sexta" : 5,
       "sabado" : 6,
       "domingo" : 7
       }

OutputDataSet["DayOfWeekNumber"] = pandas.Series([daysMap[i] for i in OutputDataSet["dia"]], index = OutputDataSet.index, dtype = "int32")
	', 
@input_data_1 = N'SELECT * FROM #DiaSemana',
@input_data_1_name = N'InputDataSet'
with result sets (("ID" int, "Dia da Semana" nvarchar(7) null,"Número do dia" int null));
GO
