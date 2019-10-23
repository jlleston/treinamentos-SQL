------------------------------------------------------------------------
------[              MODULO 4 - Índices COLUMNSTORE             ]-------
------  migração de tabelas ROWSTORE para CLUSTERED COLUMNSTORE  -------
------           criando tabelas CLUSTERED COLUMNSTORE           -------
------------------------------------------------------------------------

	use [WideWorldImportersDW]
	GO

	set nocount on;
	GO

	-- Verificando as tabelas que possuem índices COLUMNSTORE
	select
			CONCAT('[',SCHEMA_NAME(o.schema_id),'].',o.name) as tabela,
			i.name as indice,
			i.type_desc as tipo,
			MAX(cr.partition_number) as numero_particoes,
			MAX(cr.row_group_id+1) as numero_rowgroups,
			SUM(cr.total_rows) as total_registros,
			SUM(cr.deleted_rows) as total_deleted,
			(SUM(cr.size_in_bytes)/1024.0) as total_KB
	from sys.indexes i
	inner join sys.column_store_row_groups cr
		on i.object_id = cr.object_id and i.index_id = cr.index_id
	inner join sys.objects o
		on i.object_id = o.object_id
	where i.type in (5,6) -- COLUMNSTORE CLUSTERED e NONCLUSTERED
	group by o.schema_id, o.name, i.name, i.type_desc
	GO

	--> Avaliando o processo de SEGMENT ELIMINATION e performance

	-- Observando o desempenho de consulta da tabela Fact.Sale
	set statistics io on;
	GO

	-- observar o segment elimination (skipped)
	select SUM(s.Quantity * s.[Unit Price]) as TotalVendas, COUNT(*) as Vendas
	from Fact.Sale s
	inner join Dimension.Date d
		on s.[Invoice Date Key] = d.Date
	where d.[Calendar Year] = 2015
	GO

	select SUM(s.Quantity * s.[Unit Price]) as TotalVendas, COUNT(*) as Vendas
	from Fact.Sale s
	inner join Dimension.Date d
		on s.[Invoice Date Key] = d.Date
	where d.[Calendar Year] = 2016
	GO

	set statistics io off;
	GO

	--> melhorando o SEGMENT ALIGMENT
	
	-- retornando a tabela para o formato ROWSTORE
	-- e ordenando ela pelo campo mais indicado, no exemplo [Invoice Date Key]
	create clustered index CCX_Fact_Sale
		on Fact.Sale ([Invoice Date Key])
		with (DROP_EXISTING = ON);
	GO

	-- recriando o índice CLUSTERED COLUMNSTORE, agora inserindo registros na ordem do cluster ROWSTORE existente
	create clustered columnstore index CCX_Fact_Sale
		on Fact.Sale
		with (MAXDOP = 1, DROP_EXISTING = ON);
	GO

	--> Rodar novamente as queries anteriores de Total de Vendas e verificar novamente o "segment elimination"
	GO
	-->


	--> Migrando tabelas ROWSTORE para CLUSTERED COLUMNSTORE

	set statistics io on;
	GO

	-- Verificando a performance da tabela Fact.OrderHistoryExtended (29 milhões de registros)
	SELECT
			MONTH(o.[Order Date Key]) as Mes,
			o.[Stock Item Key] as ItemID,
			o.[Description] as Descricao,
			SUM(o.Quantity) as Quantidade,
			MAX(o.[Unit Price]) as ValorUnitario,
			SUM(o.[Total Excluding Tax]) as ValorVenda,
			SUM(o.[Tax Amount]) as Impostos,
			SUM(o.[Total Including Tax]) as ValorVendaFinal
	FROM Fact.OrderHistoryExtended o
	where o.[Order Date Key] between '2015-01-01' and '2015-12-31'
	GROUP BY MONTH(o.[Order Date Key]), o.[Stock Item Key], o.[Description]
	ORDER BY Mes, ValorVenda DESC;
	GO

	set statistics io off;
	GO

	-- criando o índice sugerido pelo DTA
	create nonclustered index IX_Fact_OrderHistoryExtended_Order_Date_Key
		on [Fact].[OrderHistoryExtended] ([Order Date Key])
		include ([Stock Item Key], [Description], [Quantity], [Unit Price], [Total Excluding Tax], [Tax Amount], [Total Including Tax])
	GO

	--> Verificando novamente a performance da query
	GO
	-->


	-- excluindo o índice criado anteriormente
	drop index IX_Fact_OrderHistoryExtended_Order_Date_Key on [Fact].[OrderHistoryExtended];
	GO

	-- migrando para CLUSTERED COLUMNSTORE
	create clustered columnstore index cci_Fact_OrderHistoryExtended
	on [Fact].[OrderHistoryExtended]
	with (MAXDOP = 4);
	GO

	--> Verificando novamente a performance da query
	GO
	-->


	--> Criando uma tabelas CLUSTERED COLUMNSTORE

	-- criando a tabela sem índices ou contraints
	drop table if exists OperacaoVenda;
	create table dbo.OperacaoVenda
		(
			id				bigint identity(1,1),
			nmProduto		varchar(100) not null,
			vrProduto		smallmoney not null,
			nuQuantidade	int not null,
			vrOperacao		as (vrProduto * nuQuantidade),
			dtOperacao		date,
			coStatus		varchar(12),
			deObservacao	varchar(MAX)
		);
	GO

	-- realizando a carga inicial (200 mil registros)
	insert into dbo.OperacaoVenda (nmProduto, vrProduto, nuQuantidade, dtOperacao, coStatus, deObservacao)
	select
			o.Description,
			o.[Unit Price],
			o.Quantity,
			o.[Order Date Key],
			(case when o.[Picked Date Key] is not null then 'ENTREGUE' else 'PARA ENTREGA' end) as coStatus,
			'Carga Inicial' as deObservacao
	from Fact.[Order] o
	order by o.[Order Key];
	GO

	-- criando um índice cluster para orientar a inserção de segmentos no índice columnstore
	create clustered index cci_OperacaoVenda
		on dbo.OperacaoVenda (dtOperacao);
	GO

	-- criando o índice clustered columnstore, orientado pelo rowstore devido ao drop_existing=ON
	create clustered columnstore index cci_OperacaoVenda
		on dbo.OperacaoVenda
		with (MAXDOP = 1, DROP_EXISTING = ON);
	GO

	-- criando a chave primária
	alter table dbo.OperacaoVenda
		add constraint PK_OperacaoVenda primary key nonclustered (id);
	GO

	-- criando os índices nonclustered rowstore para as FKs
	create nonclustered index ix_OperacaoVenda_dtOperacao
		on dbo.OperacaoVenda (dtOperacao) with (fillfactor=95);
	GO

	-- criando as FKs
	alter table dbo.OperacaoVenda
		add	constraint FK_OperacaoVenda_dtOperacao foreign key (dtOperacao)
			references dimension.[date] ([date]);
	GO

	-- testando o desempenho
	set statistics io on;
	set statistics time on;
	GO

	select
			d.[Calendar Year] as ano,
			d.[Month] as mes,
			o.nmProduto as Produto,
			SUM(o.vrOperacao) as TotalVendas,
			SUM(o.nuQuantidade) as QuantidadeVendida,
			COUNT(*) as NumeroOperacoesVendas
	from dbo.OperacaoVenda o
	inner join Dimension.Date d
		on o.dtOperacao = d.Date
	where d.[Calendar Year] between 2015 and 2016
	group by d.[Calendar Year], d.[Calendar Month Number], d.[Month], o.nmProduto
	order by  d.[Calendar Year], d.[Calendar Month Number], produto;
	GO

	set statistics time off;
	set statistics io off;
	GO

	drop table dbo.OperacaoVenda;
	GO
