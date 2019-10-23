--------------------------------------------------------------------
------[            MODULO 4 - Índices COLUMNSTORE           ]-------
------  uso de NONCLUSTERED COLUMNSTORE com tabela ROWSTORE  -------
--------------------------------------------------------------------

	use [master];
	GO

	--> Obtendo o caminho padrão para a criação de arquivos de dados.
	DECLARE @data_path nvarchar(256);
	SET @data_path = (SELECT SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1)
					  FROM master.sys.master_files
					  WHERE database_id = 1 AND file_id = 1);
	SELECT @data_path as caminho_padrao;
	GO

	--> Criando o database de teste WWI_Stage
	CREATE DATABASE [WWI_Stage]
	ON
		PRIMARY 
			( NAME = N'WWI_Stage', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage.mdf' , SIZE = 8MB , FILEGROWTH = 64MB ), 

		FILEGROUP [STAGEDATA] DEFAULT
			( NAME = N'WWI_Stage_data1', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage_data1.ndf' , SIZE = 256MB , MAXSIZE = 5GB , FILEGROWTH = 256MB ),
			( NAME = N'WWI_Stage_data2', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage_data2.ndf' , SIZE = 256MB , MAXSIZE = 5GB , FILEGROWTH = 256MB ),
			( NAME = N'WWI_Stage_data3', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage_data3.ndf' , SIZE = 256MB , MAXSIZE = 5GB , FILEGROWTH = 256MB ),
			( NAME = N'WWI_Stage_data4', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage_data4.ndf' , SIZE = 256MB , MAXSIZE = 5GB , FILEGROWTH = 256MB )
	LOG ON 
			( NAME = N'WWI_Stage_log', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Stage_log.ldf' , SIZE = 65536KB , MAXSIZE = 5120000KB , FILEGROWTH = 65536KB );
	GO


	use [WWI_Stage];
	GO

	set nocount on;
	GO


	--> Exemplo 1: Criando um índice NONCLUSTERED COLUMNSTORE em uma tabela ROWSTORE padrão

	-- Criando a tabela stage Orders
	create table dbo.Orders
		(
			AccountKey			int not null,
			CustomerName		nvarchar (50),
			OrderNumber			bigint,
			PurchasePrice		decimal (9,2),
			OrderStatus			smallint not NULL,
			OrderStatusDesc		nvarchar (50)
		);
	GO

	create clustered index orders_ci on dbo.Orders (OrderNumber);
	GO

	-- Inserindo 3 milhões de registros na tabela Orders
	set nocount on;
	GO

	declare @outerloop int = 0;
	declare @i int = 0;
	declare @purchaseprice decimal (9,2);
	declare @customername nvarchar (50);
	declare @accountkey int;
	declare @orderstatus smallint;
	declare @orderstatusdesc nvarchar(50);
	declare @ordernumber bigint;

	while (@outerloop < 3000000)
	begin
		set @i = 0;
		begin tran;
		while (@i < 2000)
		begin
				set @ordernumber = @outerloop + @i;
				set @purchaseprice = rand() * 1000.0;
				set @accountkey = convert (int, RAND ()*1000);
				set @orderstatus = 5;
			
				set @orderstatusdesc  =		(case @orderstatus
												when 0 then 'Order Started'
												when 1 then 'Order Closed'
												when 2 then 'Order Paid'
												when 3 then 'Order Fullfillment'
												when 4 then 'Order Shipped'
												when 5 then 'Order Received'
											end);

				insert dbo.Orders values (@accountkey,(convert(varchar(6), @accountkey) + 'firstname'),
										  @ordernumber, @purchaseprice, @orderstatus, @orderstatusdesc);
				set @i += 1;
		end;
		commit;

		set @outerloop = @outerloop + 2000;
		set @i = 0;
	end
	GO

	select count(*), OrderStatusDesc from dbo.Orders group by OrderStatusDesc;
	GO

	-- criando o NCCI (NONCLUSTERED COLUMNSTORE INDEX) sem incluir o coluna do índice clustered
	CREATE NONCLUSTERED COLUMNSTORE INDEX orders_ncci ON dbo.orders  (accountkey, customername, purchaseprice, orderstatus)
	GO

	-- analisando os rowgroups gerados
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			crg.index_id,
			crg.row_group_id,
			crg.delta_store_hobt_id,
			crg.state_desc,
			crg.total_rows,
			crg.trim_reason_desc,
			crg.transition_to_compressed_state_desc
	from sys.dm_db_column_store_row_group_physical_stats crg
	inner join sys.indexes i
		on crg.[object_id] = i.[object_id] and crg.index_id = i.index_id
	where crg.[object_id] = OBJECT_ID('dbo.orders')
	order by tabela, indice, crg.row_group_id;
	GO


	-- adicionando mais 200 mil registros
	set nocount on;
	GO

	declare @outerloop int = 3000000;
	declare @i int = 0;
	declare @purchaseprice decimal (9,2);
	declare @customername nvarchar (50);
	declare @accountkey int;
	declare @orderstatus smallint;
	declare @orderstatusdesc nvarchar(50);
	declare @ordernumber bigint;

	while (@outerloop < 3200000)
	begin
		set @i = 0;
		begin tran;
		while (@i < 2000)
		begin
				set @ordernumber = @outerloop + @i;
				set @purchaseprice = rand() * 1000.0;
				set @accountkey = convert (int, RAND ()*1000);
				set @orderstatus = convert (smallint, RAND()*5);
				if (@orderstatus = 5) set @orderstatus = 4;

				set @orderstatusdesc  =		(case @orderstatus
												when 0 then 'Order Started'
												when 1 then 'Order Closed'
												when 2 then 'Order Paid'
												when 3 then 'Order Fullfillment'
												when 4 then 'Order Shipped'
												when 5 then 'Order Received'
											end);

				insert dbo.orders values (@accountkey,(convert(varchar(6), @accountkey) + 'firstname'),
										  @ordernumber, @purchaseprice, @orderstatus, @orderstatusdesc);
				set @i += 1;
		end;
		commit;

		set @outerloop = @outerloop + 2000;
		set @i = 0;
	end;
	GO

	select count(*) from dbo.Orders;
	GO

	-- analisando os rowgroups gerados
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			crg.index_id,
			crg.row_group_id,
			crg.delta_store_hobt_id,
			crg.state_desc,
			crg.total_rows,
			crg.trim_reason_desc,
			crg.transition_to_compressed_state_desc
	from sys.dm_db_column_store_row_group_physical_stats crg
	inner join sys.indexes i
		on crg.[object_id] = i.[object_id] and crg.index_id = i.index_id
	where crg.[object_id] = OBJECT_ID('dbo.orders')
	order by tabela, indice, crg.row_group_id;
	GO


	-- analisando a performance
	CHECKPOINT
	GO
	DBCC DROPCLEANBUFFERS
	GO

	set statistics time on
	set statistics io on
	GO

	-- consulta com aggregates - usando normalmente o NCCI
	select top (5) customername as cliente, SUM (PurchasePrice) as total_compra, AVG (PurchasePrice) as media_por_compra
	from dbo.orders
	where purchaseprice > 70.0 and OrderStatus = 5
	group by customername;
	GO

	 -- usando o hint IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX para remover o NCCI da consulta
	select top (5) customername as cliente, SUM (PurchasePrice) as total_compra, AVG (PurchasePrice) as media_por_compra
	from dbo.orders
	where purchaseprice > 70.0 and OrderStatus = 5
	group by customername
	option (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX);
	GO

	-- criando o índice noncluster ROWSTORE sugerido pelo plano de execução
	CREATE NONCLUSTERED INDEX ix_teste_orders ON dbo.orders (OrderStatus, PurchasePrice)
		INCLUDE (customername);
	GO

	--> Repetir as queries com o novo índice criado
	GO
	-->

	--> Limpar ambiente
	set statistics time off
	set statistics io off
	GO
	DROP INDEX ix_teste_orders ON dbo.orders;
	GO

	--> Exemplo 2: Criando um índice NONCLUSTERED COLUMNSTORE com filtro em uma tabela ROWSTORE padrão


	-- criando a tabela Orders_filtered
	create table dbo.Orders_filtered
		(
			AccountKey			int not null,
			CustomerName		nvarchar (50),
			OrderNumber			bigint,
			PurchasePrice		decimal (9,2),
			OrderStatus			smallint,
			OrderStatusDesc		nvarchar (50)
		);
	GO

	create clustered index orders_filtered_ci on dbo.Orders_filtered (OrderNumber);
	GO

	-- inserindo 3 milhões de registros
	set nocount on
	GO

	declare @outerloop int = 0;
	declare @i int = 0;
	declare @purchaseprice decimal (9,2);
	declare @customername nvarchar (50);
	declare @accountkey int;
	declare @orderstatus smallint;
	declare @orderstatusdesc nvarchar(50);
	declare @ordernumber bigint;

	while (@outerloop < 3000000)
	begin
		set @i = 0;
		begin tran;
			while (@i < 2000)
			begin
					set @ordernumber = @outerloop + @i;
					set @purchaseprice = rand() * 1000.0;
					set @accountkey = convert (int, RAND ()*1000000);
					set @orderstatus = 5;
					
					set @orderstatusdesc  =		(case @orderstatus
													when 0 then 'Order Started'
													when 1 then 'Order Closed'
													when 2 then 'Order Paid'
													when 3 then 'Order Fullfillment'
													when 4 then 'Order Shipped'
													when 5 then 'Order Received'
												end);

					insert dbo.Orders_filtered values ( @accountkey,(convert(varchar(6), @accountkey) + 'firstname'),
														@ordernumber, @purchaseprice, @orderstatus, @orderstatusdesc);
					set @i += 1;
			end;
		commit;

		set @outerloop = @outerloop + 2000;
		set @i = 0;
	end;
	GO

	-- criando o índice NONCLUSTERED COLUMNSTORE com o filtro no campor orderstatus
	CREATE NONCLUSTERED COLUMNSTORE INDEX orders_filtered_ncci
		ON dbo.Orders_filtered  (accountkey, customername, purchaseprice, orderstatus)
		WHERE orderstatus = 5;
	GO

	select * from sys.indexes where object_id=object_id('dbo.orders_filtered');
	GO

	-- analisando os rowgroups gerados
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			crg.index_id,
			crg.row_group_id,
			crg.delta_store_hobt_id,
			crg.state_desc,
			crg.total_rows,
			crg.trim_reason_desc,
			crg.transition_to_compressed_state_desc
	from sys.dm_db_column_store_row_group_physical_stats crg
	inner join sys.indexes i
		on crg.[object_id] = i.[object_id] and crg.index_id = i.index_id
	where crg.[object_id] = OBJECT_ID('dbo.orders_filtered')
	order by tabela, indice, crg.row_group_id;
	GO

	-- recriando o índice NCCI com MAXDOP = 1
	CREATE NONCLUSTERED COLUMNSTORE INDEX orders_filtered_ncci
		ON dbo.orders_filtered  (accountkey, customername, purchaseprice, orderstatus)
		WHERE orderstatus = 5
		WITH (MAXDOP = 1, DROP_EXISTING=ON);
	GO


	-- inserindo mais 200 mil registros
	set nocount on;
	GO

	declare @outerloop int = 3000000;
	declare @i int = 0;
	declare @purchaseprice decimal (9,2);
	declare @customername nvarchar (50);
	declare @accountkey int;
	declare @orderstatus smallint;
	declare @orderstatusdesc nvarchar(50);
	declare @ordernumber bigint;

	while (@outerloop < 3200000)
	begin
		set @i = 0;
		begin tran;
		while (@i < 2000)
		begin
				set @ordernumber = @outerloop + @i;
				set @purchaseprice = rand() * 1000.0;
				set @accountkey = convert (int, RAND ()*1000000);
				set @orderstatus = convert (smallint, RAND()*5);
				if (@orderstatus = 5) set @orderstatus = 4;
			
					set @orderstatusdesc  =		(case @orderstatus
													when 0 then 'Order Started'
													when 1 then 'Order Closed'
													when 2 then 'Order Paid'
													when 3 then 'Order Fullfillment'
													when 4 then 'Order Shipped'
													when 5 then 'Order Received'
												end);

				insert dbo.orders_filtered values ( @accountkey,(convert(varchar(6), @accountkey) + 'firstname'),
													@ordernumber, @purchaseprice, @orderstatus, @orderstatusdesc);
				set @i += 1;
		end;
		commit;

		set @outerloop = @outerloop + 2000;
		set @i = 0;
	end;
	GO

	-- verificando os totais de registros
	select count(*) As [Total Rows] from dbo.Orders_filtered;
	select count(*) AS [Closed Orders] from dbo.Orders_filtered where OrderStatus = 5; -- Observe o Aggregate Pushdown
	GO

	-- Verificando em uma única chamada
	select
			COUNT(*) AS [Total Rows],
			SUM( case when OrderStatus = 5 then 1 else 0 end ) as [Closed Orders] -- usando SUM para fazer count filtrado
	from dbo.Orders_filtered;
	GO

	select sum (total_rows) --????
	from sys.dm_db_column_store_row_group_physical_stats 
	where object_id = object_id('orders_filtered')
	GO

	-- analisando os rowgroups gerados
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			crg.index_id,
			crg.row_group_id,
			crg.delta_store_hobt_id,
			crg.state_desc,
			crg.total_rows,
			crg.trim_reason_desc,
			crg.transition_to_compressed_state_desc
	from sys.dm_db_column_store_row_group_physical_stats crg
	inner join sys.indexes i
		on crg.[object_id] = i.[object_id] and crg.index_id = i.index_id
	where crg.[object_id] = OBJECT_ID('dbo.orders_filtered')
	order by tabela, indice, crg.row_group_id;
	GO


	-- analisando a performance
	CHECKPOINT
	GO
	DBCC DROPCLEANBUFFERS
	GO

	set statistics time on
	set statistics io on
	GO


	-- query com COLUMNSTORE filtrado (mas sem o uso do mesmo como predicado da consulta)
	select max (PurchasePrice)
	from dbo.Orders_filtered

	-- query com COLUMNSTORE sem filtro
	select max (PurchasePrice)
	from dbo.Orders;
	GO

	-- agora vamos comparar a query com o filtro OrderStatus = 5
	-- query com COLUMNSTORE filtrado
	select max (PurchasePrice)
	from dbo.Orders_filtered --> uso de Aggregate Pushdown
	where OrderStatus = 5;

	-- query com COLUMNSTORE sem filtro
	select max (PurchasePrice)
	from dbo.Orders
	where OrderStatus = 5;
	GO

	-- comparação com e sem NCCI
	CHECKPOINT
	GO
	DBCC DROPCLEANBUFFERS
	GO

	select top (5) customername, sum (PurchasePrice), Avg (PurchasePrice)
	from dbo.Orders_filtered
	where purchaseprice > 90.0 and OrderStatus = 5
	group by customername
 
	-- a more complex query without NCCI
	select top (5) customername, sum (PurchasePrice), Avg (PurchasePrice)
	from dbo.Orders_filtered
	where purchaseprice > 90.0 and OrderStatus = 5
	group by customername
	option (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX)
	GO

	-- criando indice nonclustered sugerido
	CREATE NONCLUSTERED INDEX ix_teste
		ON [dbo].[orders_filtered] ([OrderStatus],[PurchasePrice])
		INCLUDE ([CustomerName])
	GO

	--> realizando novo teste
	GO
	--> limpando ambiente
	set statistics time off
	set statistics io off
	GO

	DROP INDEX ix_teste ON [dbo].[orders_filtered];
	GO
	CHECKPOINT
	GO
	DBCC DROPCLEANBUFFERS
	GO


	--> Exemplo 3: Criando um índice NONCLUSTERED COLUMNSTORE em uma tabela ROWSTORE HEAP


	-- criando a tabela Accounts
	create table dbo.Accounts
		(
			accountkey			int not null,
			accountdescription	nvarchar (50),
			accounttype			nvarchar(50),
			unitsold		    int
		)
	GO

	set nocount on
	GO

	-- inserindo 10000 regisros
	declare @outerloop int = 0;
	declare @i int = 0;

	while (@outerloop < 10000)
	begin
		set @i = 0;
		begin tran;
		while (@i < 2000)
		begin
				insert dbo.Accounts values (@i + @outerloop, 'test1', 'test2', @i);
				set @i += 1;
		end;
		commit;

		set @outerloop = @outerloop + 2000;
		set @i = 0;
	end;
	GO


	--create NCCI 
	CREATE NONCLUSTERED COLUMNSTORE INDEX accounts_NCCI 
		ON dbo.Accounts (accountkey, accountdescription, unitsold)
		WITH (DROP_EXISTING=ON);
	GO

	-- analisando os rowgroups gerados
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			crg.index_id,
			crg.row_group_id,
			crg.delta_store_hobt_id,
			crg.state_desc,
			crg.total_rows,
			crg.trim_reason_desc,
			crg.transition_to_compressed_state_desc
	from sys.dm_db_column_store_row_group_physical_stats crg
	inner join sys.indexes i
		on crg.[object_id] = i.[object_id] and crg.index_id = i.index_id
	where crg.[object_id] = OBJECT_ID('dbo.accounts')
	order by tabela, indice, crg.row_group_id;
	GO

	-- analisando a estrutura do índice
	select
			object_name(i.object_id) as tabela,
			i.name as indice,
			ic.index_column_id,
			c.name as coluna,
			c.column_id as tabela_column_id
	from sys.indexes i
	inner join sys.index_columns ic
		on i.object_id = ic.object_id and i.index_id = ic.index_id
	inner join sys.columns c
		on ic.object_id = c.object_id and ic.column_id = c.column_id
	where i.object_id = OBJECT_ID('dbo.accounts')
	order by tabela, indice, ic.index_column_id;
	GO

	-- mas como a tabela é HEAP, as colunas faltantes no índice também foram incorporadas
	-- isso ocorre internamente para permitir a identificação das unicidades
	-- quando olhamos os segmentos, vemos que foram criados para as colunas "externas" ao índice 
	select
			c.name,
			s.column_id,
			s.segment_id as rowgroup_id,
			'{'+CONCAT_WS('-',s.hobt_id, s.column_id, s.segment_id)+'}' as segment_id,
			s.min_data_id,
			s.max_data_id
	from sys.column_store_segments s
	inner join sys.partitions p
		on p.hobt_id = s.hobt_id
	inner join sys.columns c
		on p.object_id = c.object_id and s.column_id = c.column_id
	where p.object_id = object_id('dbo.accounts')
	order by s.column_id
	GO


	-- mesmo com a inclusão de todos os campos, eles não serão utilizados na índice NCCI
	select avg (unitsold)
	from dbo.Accounts

	select avg (unitsold), MAX(accounttype)
	from dbo.Accounts

	select accountkey, accountdescription, unitsold
	from dbo.Accounts where unitsold between 200 and 500;

	select accountkey, accountdescription, unitsold, accounttype
	from dbo.Accounts
	where unitsold between 200 and 500;

	select accountkey, accountdescription, unitsold, accounttype
	from dbo.Accounts with (index = accounts_NCCI)
	where unitsold between 200 and 500;
	GO
