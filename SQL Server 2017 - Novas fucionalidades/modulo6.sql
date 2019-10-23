-------------------------------------------------------------------------
------[                MODULO 6 - Temporal Tables                ]-------
-------------------------------------------------------------------------

	use WideWorldImporters
	GO

	-- verifica as Temporal Tables existentes
	select
			SCHEMA_NAME(t.schema_id) as temporal_table_schema,
			t.name					 as temporal_table_name,
			SCHEMA_NAME(h.schema_id) as history_table_schema,
			h.name					 as history_table_name,
			(case when t.history_retention_period = -1 
				then 'INFINITE' 
				else CONCAT(t.history_retention_period, ' ', t.history_retention_period_unit_desc, 'S')
			 end)					 as retention_period
	from sys.tables t
	inner join sys.tables h
		on t.history_table_id = h.object_id
	where t.temporal_type = 2
	order by temporal_table_schema, temporal_table_name;
	GO


	-- Criando uma tabela versionada (System-Versioned Temporal Table)
	create table dbo.Funcionarios
		(
			IDFuncionario int identity not null CONSTRAINT PK_Funcionarios PRIMARY KEY CLUSTERED,
			Nome nvarchar(60) not null,
			NomeCracha nvarchar(20) null,
			Matricula char(6) not null,
			Telefone varchar(20) null,
			Email varchar(100) not null,
			DTContratacao date not null,
			DTDemissao date null,
			BTFuncionarioAtivo as (case when DTDemissao is null then 1 else 0 end) persisted,

			DTVersaoSTART DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
			DTVersaoEND   DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,

						  PERIOD FOR SYSTEM_TIME (DTVersaoSTART, DTVersaoEND)
		
		) WITH (SYSTEM_VERSIONING = ON);
	GO

	-- inserindo dados
	insert into dbo.Funcionarios ( Nome, NomeCracha, Matricula, Telefone, Email, DTContratacao )
	select FullName, PreferredName, FORMAT(PersonID, '0000'), PhoneNumber, EmailAddress, GETDATE()
	from Application.People
	where IsEmployee = 1;
	GO

	-- consultando a tabela
	select * from dbo.Funcionarios;
	GO

	--> Dando nome a tabela de histórico e escondendo as colunas de controle SYSTEM_TIME

	-- removendo o versionamento
	alter table dbo.Funcionarios
		SET (SYSTEM_VERSIONING = OFF);
	GO

	-- apagando a tabela historica com o nome gerado automaticamente
	DROP TABLE MSSQL_TemporalHistoryFor_292196091;
	GO

	-- colocando as colunas SYSTEM_TIME no modo HIDDEN
	alter table dbo.Funcionarios
		alter column DTVersaoSTART ADD HIDDEN;
	GO
	alter table dbo.Funcionarios
		alter column DTVersaoEND   ADD HIDDEN;
	GO

	-- adicionando novamente o versionamento, agora escolhendo o nome da tabela historica
	alter table dbo.Funcionarios
		SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.Funcionarios_Historico));
	GO

	-- consultando a tabela
	select * from dbo.Funcionarios;
	GO
	select *, DTVersaoSTART, DTVersaoEND from dbo.Funcionarios;
	GO

	-- adicionando uma nova coluna (restrição apenas para colunas calculadas)
	alter table dbo.Funcionarios
		add IDDepartamento INT null;
	GO

	-- consultando a tabela
	select *
	from dbo.Funcionarios
	where Matricula = '0015';
	GO

	update dbo.Funcionarios
		set DTDemissao = GETDATE()
	where Matricula = '0015';
	GO

	-- consultando a tabela
	select * from dbo.Funcionarios;
	GO
	select *, DTVersaoSTART, DTVersaoEND from dbo.Funcionarios;
	GO

	-- consultando o historico completo
	select * , DTVersaoSTART, DTVersaoEND
	from dbo.Funcionarios

		FOR SYSTEM_TIME ALL

	ORDER BY IDFuncionario;
	GO

	-- consultando um "snapshot" da tabela para um determinado ponto no tempo
	select * , DTVersaoSTART, DTVersaoEND
	from dbo.Funcionarios

		FOR SYSTEM_TIME AS OF '2019-09-10 22:06:07.3570660'
--		FOR SYSTEM_TIME AS OF '2019-09-10 22:06:07'
--		FOR SYSTEM_TIME AS OF '2019-09-10'

	ORDER BY IDFuncionario;
	GO

	-- Funcionario 0015 contratado e demidito no mesmo dia ... melhor apagar
	delete dbo.Funcionarios
	where Matricula = '0015';
	GO

	-- preciso reconstruir a sequencia de matricula
	update dbo.Funcionarios
		set Matricula = FORMAT( ( 14 + CAST(Matricula as int) - 15 ), '0000')
	where Matricula > '0015';
	GO

	-- consultando a tabela
	select *
	from dbo.Funcionarios
	where Matricula = '0015';
	GO

	-- vixe, achei que tinha apagado o 0015 ... vamos lá novamente
	delete dbo.Funcionarios
	where Matricula = '0015';
	GO

	-- consultando a tabela
	select *
	from dbo.Funcionarios
	where Matricula = '0015';
	GO

	-- agora sim
	-- vamos reconstruir a sequencia de matricula
	update dbo.Funcionarios
		set Matricula = FORMAT( ( 14 + CAST(Matricula as int) - 15 ), '0000')
	where Matricula > '0015';
	GO

	-- consultando a tabela
	-- OPS!!! Parece que eram 19 funcionários e só tem 17 ???????
	select * , DTVersaoSTART, DTVersaoEND
	from dbo.Funcionarios
	GO

	-- consultando o historico de carga
	select *
	from dbo.Funcionarios
		FOR SYSTEM_TIME AS OF '2019-09-10 22:00:04.8031180';
	GO

	-- cadê o "Archer Lamble"
	select
			* ,
			DTVersaoSTART as Inicio,
			DTVersaoEND as Fim

	from dbo.Funcionarios

		FOR SYSTEM_TIME ALL

	where Nome = 'Archer Lamble'
	ORDER BY Inicio ASC;
	GO

	-- confirmando suspeitas (verificar se data de fim é "9999-12-31 23:59:59.9999999")
	select
			* ,
			DTVersaoSTART as Inicio,
			DTVersaoEND as Fim

	from dbo.Funcionarios

		FOR SYSTEM_TIME ALL

	where Matricula = '0015'
	ORDER BY Inicio ASC;
	GO

	-- corrigindo as matriculas
	update dbo.Funcionarios
		set Matricula = FORMAT( ( CAST(Matricula as int) + 1 ), '0000')
	where Matricula >= '0015';
	GO

	-- consultando a tabela
	select *
	from dbo.Funcionarios
	ORDER BY Matricula;
	GO

	-- resgatando o funcionário "Archer Lamble"
	SET IDENTITY_INSERT dbo.Funcionarios ON;
	GO
	insert into dbo.Funcionarios ( IDFuncionario, Nome, NomeCracha, Matricula, Telefone, Email, DTContratacao, DTDemissao )
	select
			IDFuncionario,
			Nome,
			NomeCracha,
			Matricula,
			Telefone,
			Email,
			DTContratacao,
			DTDemissao
	from dbo.Funcionarios
		FOR SYSTEM_TIME ALL
	where IDFuncionario = 15 AND DTVersaoEND  = '2019-09-10 22:26:47.1226446';
	GO
	SET IDENTITY_INSERT dbo.Funcionarios OFF;
	GO

	-- consultando a tabela
	select *
	from dbo.Funcionarios
	ORDER BY Matricula;
	GO


	-- consultando um período historico da tabela
	select * , DTVersaoSTART, DTVersaoEND
	from dbo.Funcionarios

		FOR SYSTEM_TIME FROM '2019-09-10 22:00:04.8031180' TO '2019-09-10 22:00:05'

	ORDER BY IDFuncionario;
	GO

	declare @agora datetime2 = SYSUTCDATETIME();
	select * , DTVersaoSTART as Inicio, DTVersaoEND as Fim
	from dbo.Funcionarios

		FOR SYSTEM_TIME FROM    '2019-09-10 22:00:04.8031180' TO @agora
		--FOR SYSTEM_TIME BETWEEN '2019-09-10 22:00:04.8031180' AND @agora
		--FOR SYSTEM_TIME CONTAINED IN ('2019-09-10 22:00:04.8031180' , @agora)

	ORDER BY Inicio;
	GO

	-- é possível consultar a tabela histórica de forma direta
	select *
	from dbo.Funcionarios_Historico;
	GO

	-- não é possivel alterar a tabela histórica
	delete from dbo.Funcionarios_Historico;
	GO
	truncate table dbo.Funcionarios_Historico;
	GO
	update dbo.Funcionarios_Historico
		set DTDemissao = GETDATE();
	GO

	--> Transformando uma tabela convencional em temporal

	-- criando um schema só para o histórico
	CREATE SCHEMA History;
	GO

	-- adicionando as colunas de controle periódico
	ALTER TABLE Sales.Invoices
	ADD 
		DTInicio datetime2 GENERATED ALWAYS AS ROW START HIDDEN
			CONSTRAINT DF_Invoices_DTInicio DEFAULT SYSUTCDATETIME(),

		DTFim datetime2 GENERATED ALWAYS AS ROW END HIDDEN
			CONSTRAINT DF_Invoices_DTFim DEFAULT CAST('9999-12-31 23:59:59.9999999' as datetime2),

		PERIOD FOR SYSTEM_TIME (DTInicio, DTFim);
	GO

	ALTER TABLE Sales.Invoices   
		SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = History.Invoices));
	GO




	--> Preparando uma massa histórica (COLD DATA) de alta performance analítica
	use [master]
	GO

	ALTER DATABASE [WideWorldImporters] ADD FILEGROUP [COLDDATA];
	GO

	ALTER DATABASE [WideWorldImporters]
		ADD FILE ( NAME = N'WWI_Historico1', FILENAME = N'D:\SQLDATA\MSSQL14.SQLDB17\MSSQL\DATA\WWI_Historico1.ndf' , SIZE = 1048576KB , FILEGROWTH = 1048576KB ) TO FILEGROUP [COLDDATA]
	GO

	use WideWorldImporters
	GO

	-- criando uma tabela para o COLD DATA baseada na estrutura da sua tabela a ser versionada
	drop table if exists History.CustomerTransactions;
	GO

	select TOP(0) *
	INTO History.CustomerTransactions ON [COLDDATA]
	from Sales.CustomerTransactions;
	GO

	-- se for uma base de DW ou se for prevista a consulta dos dados COLD de forma agregada, usamos a columnstore
	-- adicionando os dois campos de controle peródico manualmente
	alter table History.CustomerTransactions
		add DTInicio datetime2 not null,
			DTFim    datetime2 not null;
	GO
	-- criamos uma colunstore para as consultas analíticas futuras
	create clustered columnstore index cci_CustomerTransactions
		on History.CustomerTransactions;
	GO

	-- habilitamos o versionamento
	alter table Sales.CustomerTransactions
		add
			DTInicio datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL
					 constraint DF_CustomerTransactions_DTInicio default SYSUTCDATETIME(),

			DTFim	 datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL
					 constraint DF_CustomerTransactions_DTFim default CAST('9999-12-31 23:59:59.9999999' as datetime2),

		PERIOD FOR SYSTEM_TIME (DTInicio, DTFim);
	GO

	alter table Sales.CustomerTransactions
		SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = History.CustomerTransactions));
	GO

	-- e dois nonclustered rowstore com os campos SYSTEM_TIME, um no sentido START->END e outro inverso END->START
	-- incluimos como terceira coluna o campo que representa a chave cluster na tabela principal
	create nonclustered index ix_CustomerTransactions_start_end
		on History.CustomerTransactions (DTInicio, DTFim, CustomerTransactionID);
	GO

	create nonclustered index ix_CustomerTransactions_end_start
		on History.CustomerTransactions (DTFim, DTInicio, CustomerTransactionID);
	GO

	-- e criamos os dois mesmos índices de colunas SYSTEM_TIME na tabela principal (a chave cluster é adicionada automaticamente)
	create nonclustered index ix_CustomerTransactions_start_end
		on Sales.CustomerTransactions (DTInicio, DTFim);
	GO

	create nonclustered index ix_CustomerTransactions_end_start
		on Sales.CustomerTransactions (DTFim, DTInicio);
	GO


	-- agora iremos mover o COLD DATA
	-- consultando os dados
	select *
	from Sales.CustomerTransactions
	where
			TransactionDate < '2016-01-01'
		AND FinalizationDate is not null;
	GO

	-- agora excluimos os registros (se o volume for muito grande, recomendamos fazer em BATCHES
	-- neste exemplo usaremos BATCHES de 10000 registros (como são mais de 84.000, faremos 9 execuções)
	delete TOP(10000)
	from Sales.CustomerTransactions
	where
			TransactionDate < '2016-01-01'
		AND FinalizationDate is not null;
	GO 9

	-- Consultando a tabela
	select *
	from Sales.CustomerTransactions -- somente HOT DATA, 12809 registros
	order by TransactionDate;
	GO

	-- Agora, consultamos o histórico usando a versão COLD DATA
	-- Neste exemplo, poderiamos também criar um índice nonclustered na tabela HOT DATA para aumentar a performance da agregação
	select
			YEAR(TransactionDate) as ano,
			MONTH(TransactionDate) as mes,
			SUM(AmountExcludingTax) as TransacaoSemTaxas,
			SUM(TaxAmount) as TotalTaxas,
			SUM(TransactionAmount) as TotalTransacoes

	from Sales.CustomerTransactions

				FOR SYSTEM_TIME ALL

	group by YEAR(TransactionDate), MONTH(TransactionDate)
	order by YEAR(TransactionDate) DESC, MONTH(TransactionDate) DESC;
	GO



	--> Temporarl In-Memory

	-- verifica as Temporal Tables In-Memory existentes
	select SCHEMA_NAME (T1.schema_id) as TemporalTableSchema
		, OBJECT_NAME(IT.parent_object_id) as TemporalTableName
		, T1.object_id as TemporalTableObjectId
		, IT.Name as InternalHistoryStagingName
		, SCHEMA_NAME (T2.schema_id) as HistoryTableSchema
		, OBJECT_NAME (T1.history_table_id) as HistoryTableName
	from sys.internal_tables IT
	inner join sys.tables T1
		on IT.parent_object_id = T1.object_id
	inner join sys.tables T2
		on T1.history_table_id = T2.object_id
	where T1.is_memory_optimized = 1 AND T1.temporal_type = 2;
	GO

	-- criando uma Temporal Tables In-Memory
	create table dbo.FuncionariosInMemory
		(
			IDFuncionario int identity(1,1) not null CONSTRAINT PK_FuncionariosInMemory PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 64),
			Nome nvarchar(60) not null,
			NomeCracha nvarchar(20) null,
			Matricula char(6) not null,
			Telefone varchar(20) null,
			Email varchar(100) not null,
			DTContratacao date not null,
			DTDemissao date null,
			BTFuncionarioAtivo as (case when DTDemissao is null then 1 else 0 end) persisted,

			DTVersaoSTART DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
			DTVersaoEND   DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,

						  PERIOD FOR SYSTEM_TIME (DTVersaoSTART, DTVersaoEND)
		
		) 
	with
		  ( 
			  MEMORY_OPTIMIZED = ON , DURABILITY = SCHEMA_AND_DATA,
			  SYSTEM_VERSIONING = ON (	HISTORY_TABLE = dbo.FuncionariosInMemory_History,
										DATA_CONSISTENCY_CHECK = ON,
										HISTORY_RETENTION_PERIOD = 6 MONTHS)
		  );
	GO

	-- verifica as Temporal Tables In-Memory existentes
	select SCHEMA_NAME (T1.schema_id) as TemporalTableSchema
		, OBJECT_NAME(IT.parent_object_id) as TemporalTableName
		, T1.object_id as TemporalTableObjectId
		, IT.Name as InternalHistoryStagingName
		, SCHEMA_NAME (T2.schema_id) as HistoryTableSchema
		, OBJECT_NAME (T1.history_table_id) as HistoryTableName
	from sys.internal_tables IT
	inner join sys.tables T1
		on IT.parent_object_id = T1.object_id
	inner join sys.tables T2
		on T1.history_table_id = T2.object_id
	where T1.is_memory_optimized = 1 AND T1.temporal_type = 2;
	GO

	-- se a retenção não estiver habilitada no database, precisamos habilitá-la
	SELECT is_temporal_history_retention_enabled, name
	FROM sys.databases;
	GO

	ALTER DATABASE WideWorldImporters
		SET TEMPORAL_HISTORY_RETENTION ON;
	GO

	insert into dbo.FuncionariosInMemory
		(Nome, NomeCracha, Matricula, Telefone, Email, DTContratacao, DTDemissao)
	select
			f.Nome, f.NomeCracha, f.Matricula, f.Telefone, f.Email, f.DTContratacao, f.DTDemissao
	from dbo.Funcionarios f;
	GO

	select * from dbo.FuncionariosInMemory;
	GO

	update dbo.FuncionariosInMemory
		set Matricula = 'IM' + TRIM(Matricula);
	GO

	select * from dbo.FuncionariosInMemory
		FOR SYSTEM_TIME ALL;
	GO


	-- Monitorando o consumo de memória das tabelas In-Memory com Temporal
	WITH InMemoryTemporalTables
	AS
	   (
		  SELECT SCHEMA_NAME ( T1.schema_id ) AS TemporalTableSchema
			 , T1.object_id AS TemporalTableObjectId
			 , IT.object_id AS InternalTableObjectId
			 , OBJECT_NAME ( IT.parent_object_id ) AS TemporalTableName
			 , IT.Name AS InternalHistoryStagingName
		  FROM sys.internal_tables IT
		  JOIN sys.tables T1 ON IT.parent_object_id = T1.object_id
		  WHERE T1.is_memory_optimized = 1 AND T1.temporal_type = 2
	   )
	SELECT
	   TemporalTableSchema
	   , T.TemporalTableName
	   , T.InternalHistoryStagingName,
		  CASE
			 WHEN C.object_id = T.TemporalTableObjectId
			 THEN 'Temporal Table Consumption'
			 ELSE 'Internal Table Consumption'
			 END ConsumedBy
	   , C.*
	FROM sys.dm_db_xtp_memory_consumers C
	JOIN InMemoryTemporalTables T
	   ON C.object_id = T.TemporalTableObjectId OR C.object_id = T.InternalTableObjectId
	   WHERE
				T.TemporalTableSchema = 'dbo'
			AND T.TemporalTableName = 'FuncionariosInMemory';

	GO

	-- Consumo sumarizado
	WITH InMemoryTemporalTables
	AS
	   (
		  SELECT SCHEMA_NAME ( T1.schema_id ) AS TemporalTableSchema
			 , T1.object_id AS TemporalTableObjectId
			 , IT.object_id AS InternalTableObjectId
			 , OBJECT_NAME ( IT.parent_object_id ) AS TemporalTableName
			 , IT.Name AS InternalHistoryStagingName
		  FROM sys.internal_tables IT
		  JOIN sys.tables T1 ON IT.parent_object_id = T1.object_id
		  WHERE T1.is_memory_optimized = 1 AND T1.temporal_type = 2
	   )
	, DetailedConsumption
	AS
	(
	   SELECT TemporalTableSchema
		  , T.TemporalTableName
		  , T.InternalHistoryStagingName
		  , CASE
			 WHEN C.object_id = T.TemporalTableObjectId
			 THEN 'Temporal Table Consumption'
			 ELSE 'Internal Table Consumption'
			 END ConsumedBy
		  , C.*
	   FROM sys.dm_db_xtp_memory_consumers C
	   JOIN InMemoryTemporalTables T
	   ON C.object_id = T.TemporalTableObjectId OR C.object_id = T.InternalTableObjectId
	)
	SELECT
		TemporalTableSchema,
		TemporalTableName,
		SUM ( allocated_bytes ) AS allocated_bytes,
		SUM ( used_bytes ) AS used_bytes
	FROM DetailedConsumption
	WHERE
				TemporalTableSchema = 'dbo'
			AND TemporalTableName = 'FuncionariosInMemory'
	GROUP BY TemporalTableSchema, TemporalTableName	;
	GO
