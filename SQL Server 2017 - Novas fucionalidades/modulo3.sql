-----------------------------------------------
------[ MODULO 3 - In-Memory OLTP ]-----
-----------------------------------------------

	use [WideWorldImporters]
	GO

--> Criando Tabelas In-Memory
------------------------------

	--> verificando se MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT está habilitado
	select is_memory_optimized_elevate_to_snapshot_on
	from sys.databases
	where name = DB_NAME();
	GO

	-- se não estiver, podemos habilitá-lo 
	alter database WideWorldImporters set MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT=ON;
	GO

	--> Lista a tabelas in-memory já existentes
	select *
	from sys.tables
	where is_memory_optimized = 1;
	GO
	
	--> Verifica o modo de CHECKPOINT do nosso servidor
	EXEC sys.xp_readerrorlog 0, 1, N'In-Memory OLTP initialized on';
	GO


	-- Cria a tabela in-memory com durabilidade em disco
	create table dbo.Funcionarios
		(
			idFuncionario int IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,  
			nmFuncionario varchar(100) not null CHECK ( TRIM(nmFuncionario) <> '' ),
			nmCidade varchar(50) not null,
			sgEstado char(2) not null,
			dtContratacao date not null,
			dtDemissao date null,
			CONSTRAINT ck_funcionario_dtDemissao CHECK ( dtDemissao is null or dtDemissao > dtContratacao )
		)
	with
		( MEMORY_OPTIMIZED = ON,  DURABILITY = SCHEMA_AND_DATA);
	GO

	-- Verifica o consumo de memória da tabela Funcionarios
	select * 
	from sys.dm_db_xtp_table_memory_stats  
	where object_id = object_id('dbo.Funcionarios')  ;

	--> Insere dados na tabela de Funcionarios
	insert into dbo.Funcionarios
		( nmFuncionario, nmCidade, sgEstado, dtContratacao, dtDemissao )
	values
			( 'José', 'Anapolis', 'GO', GETDATE(), NULL ),
			( 'João', 'Goiânia', 'GO', '2019-01-02', NULL),
			( 'Anna', 'Brasília', 'DF', '2019-01-02', NULL),
			( 'Maria', 'Aparecida de Goiânia', 'GO', GETDATE(), NULL ),
			( 'Sandro', 'Belo Horizonte', 'MG', '2018-10-10', GETDATE() );
	GO

	-- Verifica o consumo de memória da tabela Funcionarios
	select * 
	from sys.dm_db_xtp_table_memory_stats  
	where object_id = object_id('dbo.Funcionarios')  ;
	GO

	-- Altera a tabela in-memory e adiciona um campo calculado
	alter table dbo.Funcionarios
		add blAtivo as (case when dtDemissao is null then 1 else 0 end);
	GO

	select *
	from dbo.Funcionarios;
	GO

	-- Altera a tabela in-memory e adiciona um campo com default (já preenchidos) e outro campo tipo datetime
	alter table dbo.Funcionarios
		add blExameMedicoAnual bit not null default 0 with values,
			dtUltimoExame datetime null;
	GO

	select *
	from dbo.Funcionarios;
	GO

	-- Altera a tabela in-memory e altera o tipo do campo dtUltimoExame
	alter table dbo.Funcionarios
		alter column dtUltimoExame date null;
	GO

	-- Verifica as estatística e os objetos criados na tabela in-memory
	SELECT
			object_name(c.object_id) AS table_name,
			a.xtp_object_id,
			a.type_desc, 
			minor_id,
			c.memory_consumer_id as consumer_id, 
			c.memory_consumer_type_desc as consumer_type_desc, 
			c.memory_consumer_desc as consumer_desc, 
			c.allocated_bytes as bytes,
			i.name
	FROM sys.memory_optimized_tables_internal_attributes a 
	INNER JOIN sys.dm_db_xtp_memory_consumers c 
		ON a.object_id = c.object_id and a.xtp_object_id = c.xtp_object_id 
	LEFT JOIN sys.indexes i 
		ON c.object_id = i.object_id 
			AND c.index_id = i.index_id
	WHERE c.object_id = object_id('dbo.Funcionarios'); 
	GO

	--> Exclusão da tabela dbo.Funcionarios
	drop table dbo.Funcionarios;
	GO



--> Teste de Tabelas In-Memory vs Disk-based
---------------------------------------------

	-- Cria os schemas para os testes
	create schema [OnDisk];
	GO
	create schema [InMemory];
	GO

	-- Crias as tabelas Disk-based e In-Memory para os testes
	create table OnDisk.VehicleLocations
		(
			VehicleLocationID bigint IDENTITY(1,1) PRIMARY KEY CLUSTERED,
			RegistrationNumber nvarchar(20) NOT NULL,
			TrackedWhen datetime2 NOT NULL,
			Longitude decimal(18,4) NOT NULL,
			Latitude decimal(18,4) NOT NULL
		);
	GO

	create table InMemory.VehicleLocations
		(
			VehicleLocationID bigint IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
			RegistrationNumber nvarchar(20) NOT NULL,
			TrackedWhen datetime2 NOT NULL,
			Longitude decimal(18,4) NOT NULL,
			Latitude decimal(18,4) NOT NULL
		)
	with (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
	GO

	-- Cria a SP que será usada na carga das tabela em disco
	create procedure OnDisk.InsertVehicleLocation
		@RegistrationNumber nvarchar(20),
		@TrackedWhen datetime2(2),
		@Longitude decimal(18,4),
		@Latitude decimal(18,4)
	with execute as owner
	as
	begin
		set nocount on;
		set xact_abort on;

		insert OnDisk.VehicleLocations
			(RegistrationNumber, TrackedWhen, Longitude, Latitude)
		values
			(@RegistrationNumber, @TrackedWhen, @Longitude, @Latitude);

	end;
	GO

	-- Cria a NCSP que será usada na carga das tabela in-memory
	create procedure InMemory.InsertVehicleLocation
		@RegistrationNumber nvarchar(20),
		@TrackedWhen datetime2(2),
		@Longitude decimal(18,4),
		@Latitude decimal(18,4)
	with native_compilation, schemabinding, execute as owner
	as
	begin atomic with (transaction isolation level = snapshot, language = N'English')

		insert InMemory.VehicleLocations
			(RegistrationNumber, TrackedWhen, Longitude, Latitude)
		values
			(@RegistrationNumber, @TrackedWhen, @Longitude, @Latitude);

	end;
	GO

	-- Carga da tabela em disco - 500.000 registros
	declare @start datetime2
	set @start = SYSDATETIME()

	declare @RegistrationNumber nvarchar(20);
	declare @TrackedWhen datetime2(2);
	declare @Longitude decimal(18,4);
	declare @Latitude decimal(18,4);

	declare @Counter int = 0;
	set nocount on;

	while @Counter < 500000
	begin

		-- dados gerados aleatoriamente
		set @RegistrationNumber = N'EA' + RIGHT(N'00' + CAST(@Counter % 100 as nvarchar(10)), 3) + N'-GL';
		set @TrackedWhen = SYSDATETIME();
		set @Longitude = RAND() * 100;
		set @Latitude = RAND() * 100;

		EXEC OnDisk.InsertVehicleLocation @RegistrationNumber, @TrackedWhen, @Longitude, @Latitude;

		set @Counter = @Counter + 1;

	end;

	-- tempo decorrido
	select datediff(ms,@start, sysdatetime()) as 'insert into disk-based table (in ms)'
	GO

	-- Carga da tabela em in-memory - 500.000 registros
	declare @start datetime2
	set @start = SYSDATETIME()

	declare @RegistrationNumber nvarchar(20);
	declare @TrackedWhen datetime2(2);
	declare @Longitude decimal(18,4);
	declare @Latitude decimal(18,4);

	declare @Counter int = 0;
	set nocount on;

	while @Counter < 500000
	begin
		-- dados gerados aleatoriamente
		set @RegistrationNumber = N'EA' + RIGHT(N'00' + CAST(@Counter % 100 as nvarchar(10)), 3) + N'-GL';
		set @TrackedWhen = SYSDATETIME();
		set @Longitude = RAND() * 100;
		set @Latitude = RAND() * 100;

		EXEC InMemory.InsertVehicleLocation @RegistrationNumber, @TrackedWhen, @Longitude, @Latitude;

		set @Counter = @Counter + 1;

	end;

	-- tempo decorrido
	select datediff(ms,@start, sysdatetime()) as 'insert into memory-optimized table (in ms)'
	GO
	/*
	select COUNT(*) from InMemory.VehicleLocations
	delete InMemory.VehicleLocations
	*/

	-- Comparando os dois tempos decorridos, percebemos que o insert in-memory foi mais rápido,
	-- mas ainda assim a operação ocorreu no que se chama de "interop layer", ou seja, parte do
	-- código executado era interpretado (T-SQL convencional).
	-- Vamos tentar uma operação 100% NCSP (Natively-Compiled Stored Procedure)

	create or alter procedure InMemory.Insert500ThousandVehicleLocations
	with native_compilation, schemabinding, execute as owner
	as
	begin atomic with (transaction isolation level = snapshot, language = N'English')

		declare @RegistrationNumber nvarchar(20);
		declare @TrackedWhen datetime2(2);
		declare @Longitude decimal(18,4);
		declare @Latitude decimal(18,4);

		declare @Counter int = 0;
		declare @CounterMod nvarchar(10);

		while @Counter < 500000
		begin
			-- dados gerados aleatoriamente
			set @CounterMod = '00' + CAST(@Counter % 100 as nvarchar(10));
			set @RegistrationNumber = N'EA' + substring(@CounterMod,(1 + len(@CounterMod) % 3),3) + N'-GL';
			set @TrackedWhen = SYSDATETIME();
			set @Longitude = RAND() * 100;
			set @Latitude = RAND() * 100;

			EXEC InMemory.InsertVehicleLocation @RegistrationNumber, @TrackedWhen, @Longitude, @Latitude;

			set @Counter = @Counter + 1;

		end;

	end;
	GO

	-- agora vamos executar a NCSP
	declare @start datetime2
	set @start = SYSDATETIME()

	EXEC InMemory.Insert500ThousandVehicleLocations

	-- tempo decorrido
	select datediff(ms,@start, sysdatetime()) as 'insert into memory-optimized table using native compilation (in ms)'
	GO

	/*
		-- meus resultados
		disco = 		178139 - quase 3 minutos
		in-memory = 	135400 - pouco mais de 2 minutos
		in-memory NCSP =  5369 - pouco mais de 5 segundos
	*/

	-- verificando o consumo do database (inclsive in-memory)
	EXEC sp_spaceused 
		@updateusage = 'FALSE', 
		@mode = 'ALL', 
		@oneresultset = '1', 
		@include_total_xtp_storage = '1'; -- este parâmetro deve ser passado para vermos os dados dos objetos in-memory
	GO

	-- verificando o uso de memória de objetos in-memory (run-time)
	select
		db_id() as [db_id],
		type clerk_type, 
		name, 
		memory_node_id, 
		pages_kb / 1024 pages_mb -- consumo de dados
	from sys.dm_os_memory_clerks
	where type like '%xtp%';

	select SUM(allocated_bytes) / (1024 * 1024) AS total_allocated_MB, 
		   SUM(used_bytes) / (1024 * 1024) AS total_used_MB
	from sys.dm_xtp_system_memory_consumers; 
	GO

	--> Testando a leitura de registros

	-- limpar os caches
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	alter database scoped configuration CLEAR PROCEDURE_CACHE;
	GO

	declare @start datetime2
	set @start = SYSDATETIME()

	select
			VehicleLocationID,
			RegistrationNumber,
			TrackedWhen,
			Latitude,
			Longitude
	from OnDisk.VehicleLocations
	where latitude between 70 and 80
	and longitude between 50 and 60
	order by RegistrationNumber;

	select datediff(ms,@start, sysdatetime()) as 'select disk-based table (in ms)'
	set @start = SYSDATETIME()

	select
			VehicleLocationID,
			RegistrationNumber,
			TrackedWhen,
			Latitude,
			Longitude
	from InMemory.VehicleLocations
	where latitude between 70 and 80
	and longitude between 50 and 60
	order by RegistrationNumber;

	select datediff(ms,@start, sysdatetime()) as 'select memory-optimized table (in ms)'
	GO

	--> Criando os devidos índices para melhorar as consultas
	create nonclustered index IX_OnDisk_VehicleLocations
		on OnDisk.VehicleLocations (Longitude, Latitude);
	GO

	-- determinando a melhor ordem dos campos do índice
	select COUNT(distinct latitude) as cntLat,  COUNT(distinct longitude) as cntLon  from InMemory.VehicleLocations

	alter table InMemory.VehicleLocations
		add index IX_InMemory_VehicleLocations
			nonclustered (Longitude, Latitude);
	GO

	drop index if exists IX_OnDisk_VehicleLocations on OnDisk.VehicleLocations;
	GO
	create nonclustered index IX_OnDisk_VehicleLocations
		on OnDisk.VehicleLocations (Longitude, Latitude)
			include (RegistrationNumber, TrackedWhen);
	GO
	
	-- realizando o teste com um índice HASH
	alter table InMemory.VehicleLocations
		add index IX_InMemory_VehicleLocations_HASH
			hash (Longitude, Latitude) with (BUCKET_COUNT = 100000);
	GO

	alter table InMemory.VehicleLocations
		drop index IX_InMemory_VehicleLocations_HASH
	GO


	-- criando uma SPs para ler a tabela VehicleLocations
	create or alter procedure dbo.usp_OnDisk_VehicleLocations
		(@LatMin decimal(18,4), @LatMax decimal(18,4), @LonMin decimal(18,4), @LonMax decimal(18,4))
	as
	begin
		select
			VehicleLocationID,
			RegistrationNumber,
			TrackedWhen,
			Latitude,
			Longitude
		from OnDisk.VehicleLocations
		where latitude between @LatMin and @LatMax
		and longitude between @LonMin and @LonMax
		order by RegistrationNumber;
	end;
	GO

	create or alter procedure ncsp_InMemory_VehicleLocations
		(@LatMin decimal(18,4), @LatMax decimal(18,4), @LonMin decimal(18,4), @LonMax decimal(18,4))
	with native_compilation, schemabinding
	as
	begin atomic with (transaction isolation level = snapshot, language = N'english')
		select
			VehicleLocationID,
			RegistrationNumber,
			TrackedWhen,
			Latitude,
			Longitude
		from InMemory.VehicleLocations
		where latitude between @LatMin and @LatMax
		and longitude between @LonMin and @LonMax
		order by RegistrationNumber;
	end;
	GO

	--> Testando a leitura de registros

	-- limpar os caches
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	alter database scoped configuration CLEAR PROCEDURE_CACHE;
	GO

	declare @start datetime2
	set @start = SYSDATETIME()

	exec dbo.usp_OnDisk_VehicleLocations 70,80,50,60;

	select datediff(ms,@start, sysdatetime()) as 'select disk-based table (in ms)'
	set @start = SYSDATETIME()

	exec dbo.ncsp_InMemory_VehicleLocations 70,80,50,60;

	select datediff(ms,@start, sysdatetime()) as 'select memory-optimized table (in ms)'
	GO


	-- limpar tabelas
	drop procedure if exists InMemory.Insert500ThousandVehicleLocations;
	drop procedure if exists InMemory.InsertVehicleLocation;
	drop procedure if exists OnDisk.InsertVehicleLocation;
	drop procedure if exists ncsp_InMemory_VehicleLocations;
	drop procedure if exists usp_OnDisk_VehicleLocations;
	drop table if exists InMemory.VehicleLocations;
	drop table if exists OnDisk.VehicleLocations;
	GO
	drop schema if exists InMemory;
	drop schema if exists OnDisk;
	GO


--> Criando indices HASH nas Tabelas In-Memory
----------------------------------------------

	-- criando uma tabela com uma primary key HASH com BUCKET_COUNT = 4 (muito baixo)
	create table dbo.Customers
		(
			CustomerId        int not null,
			CustomerCode      nvarchar(10) not null,
			CustomerName      nvarchar(50) not null,
			CustomerAddress   nvarchar(50) not null,
			ChkSum            int not null 
			PRIMARY KEY NONCLUSTERED HASH (customerid) WITH (BUCKET_COUNT = 4)
		)
	with (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA)
	GO

	-- estísticas dos índices hash
	select
			OBJECT_NAME(hs.object_id) as tabela,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			i.is_primary_key,
			i.is_unique,
			CONCAT('(',
				STRING_AGG(c.name + ' ' + (case when ic.is_descending_key = 1 then 'DESC' else 'ASC' end), ', ')
				WITHIN GROUP (ORDER BY ic.index_column_id ASC),
			')') as colunas
	from sys.dm_db_xtp_hash_index_stats hs
	inner join sys.hash_indexes i
		on hs.object_id = i.object_id
			and hs.index_id = i.index_id
	inner join sys.index_columns ic
		on i.object_id = ic.object_id
			and i.index_id = ic.index_id
	inner join sys.columns c
		on ic.object_id = c.object_id
			and ic.column_id = c.column_id
	where hs.object_id = OBJECT_ID('dbo.Customers')
	group by 
			hs.object_id,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			i.is_primary_key,
			i.is_unique
	GO

	-- fazendo carga de 99999 registros
	declare @i  int = 1
	while @i < 100000
	begin
		insert into dbo.customers
		select @i,  
		CONVERT(VARCHAR(10), GETDATE() ,13 ),
		CONVERT(VARCHAR(12), GETDATE() , 103 ),
		CONVERT(VARCHAR(12), GETDATE() , 103 ),
		CHECKSUM(GETDATE() )
		SET @i = @i +1
	end;
	GO

	-- ver novamente as estatísticas e verificando a longa cadeia de encadeamento (BUCKET CHAIN)
	GO

	BEGIN TRANSACTION
		update dbo.Customers with (snapshot) -- devemos usar este HINT se MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT não estiver ativo 
		   set ChkSum = 0
		 where CustomerId BETWEEN 1000 AND 3000
	GO 5

	-- ver novamente as estatísticas e verificando a nova longa cadeia de encadeamento (BUCKET CHAIN)
	GO

	ROLLBACK TRANSACTION
	GO

	-- verificando se o BUCKET_COUNT está adequado
	-- resultado abaixo de 1 significa baixo
	-- resultado entre 1 e 1.5 está adequado para o número de registros atuais
	-- resultado entre 1.5 e 2 é o ideal para pouco/médio movimento
	-- resultado acima de 2 está superestimado ou está previsto uma carga ou movimento no futuro
	declare @bucket_count decimal (9,2);
	set @bucket_count = (select top (1) [bucket_count] from sys.hash_indexes where object_id = OBJECT_ID('dbo.Customers'));
	select @bucket_count / count(*) as 'Bucket Chain Rate'
	from (select distinct customerId from dbo.Customers) as d; -- indicando a tabela e as colunas do índice
	GO

	-- verificando o valor ideal para o BUCKET_COUNT (baseado no número de registros atuais)
	select
	  POWER( 2, CEILING( LOG( COUNT(*)) / LOG( 2))) as 'BUCKET_COUNT atual',
	  POWER( 2, CEILING( LOG( COUNT(*)) / LOG( 2)) + 1) as 'BUCKET_COUNT futuro'
	from (select distinct customerId from dbo.Customers) as d; -- indicando a tabela e as colunas do índice
	GO

	-- alterando o BUCKET_COUNT para o valor ideal
	alter table dbo.Customers
		alter index [PK__Customer__A4AE64D9FAD39253] -- atualizar o nome, pois foi gerado automáticamente
			rebuild WITH (BUCKET_COUNT = 30000) -- o valor é automaticamente ajustado para a base 2
			--rebuild WITH (BUCKET_COUNT = 60000)
			--rebuild WITH (BUCKET_COUNT = 40000)
	GO

	-- estísticas dos índices hash
	select
			OBJECT_NAME(hs.object_id) as tabela,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			FLOOR((CAST(empty_bucket_count as float)/total_bucket_count) * 100) AS 'empty_bucket_percent',
			i.is_primary_key,
			i.is_unique,
			CONCAT('(',
				STRING_AGG(c.name + ' ' + (case when ic.is_descending_key = 1 then 'DESC' else 'ASC' end), ', ')
				WITHIN GROUP (ORDER BY ic.index_column_id ASC),
			')') as colunas
	from sys.dm_db_xtp_hash_index_stats hs
	inner join sys.hash_indexes i
		on hs.object_id = i.object_id
			and hs.index_id = i.index_id
	inner join sys.index_columns ic
		on i.object_id = ic.object_id
			and i.index_id = ic.index_id
	inner join sys.columns c
		on ic.object_id = c.object_id
			and ic.column_id = c.column_id
	where hs.object_id = OBJECT_ID('dbo.Customers')
	group by 
			hs.object_id,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			i.is_primary_key,
			i.is_unique
	GO

	-- limpando o ambiente
	drop table dbo.Customers
	GO

--> Identificando os indices ideais para uma Tabela In-Memory
--------------------------------------------------------------

	create table dbo.SalesOrder
		(  
			SalesOrderID uniqueidentifier not null DEFAULT ( NEWID() )
  							PRIMARY KEY NONCLUSTERED HASH WITH ( BUCKET_COUNT = 262144 ),
			OrderSequence int not null,
			OrderDate datetime2 not null,
			[Status] tinyint not null,
			INDEX IX_OrderSequence HASH (OrderSequence) WITH ( BUCKET_COUNT = 20000),
			INDEX IX_Status HASH ([Status]) WITH ( BUCKET_COUNT = 8),
			INDEX IX_OrderDate NONCLUSTERED (OrderDate ASC)
		)
	with ( MEMORY_OPTIMIZED = ON , DURABILITY = SCHEMA_AND_DATA )  
	GO  
  
	declare @i int = 0;
	BEGIN TRAN;

		while @i < 262144
		begin
		   insert dbo.SalesOrder (OrderSequence, OrderDate, [Status]) VALUES (@i, sysdatetime(), @i % 8);
		   set @i += 1 ;
		end;

	COMMIT;
	GO

	select
			OBJECT_NAME(hs.object_id) as tabela,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			FLOOR((CAST(empty_bucket_count as float)/total_bucket_count) * 100) AS 'empty_bucket_percent',
			i.is_primary_key,
			i.is_unique,
			CONCAT('(',
				STRING_AGG(c.name + ' ' + (case when ic.is_descending_key = 1 then 'DESC' else 'ASC' end), ', ')
				WITHIN GROUP (ORDER BY ic.index_column_id ASC),
			')') as colunas
	from sys.dm_db_xtp_hash_index_stats hs
	inner join sys.hash_indexes i
		on hs.object_id = i.object_id
			and hs.index_id = i.index_id
	inner join sys.index_columns ic
		on i.object_id = ic.object_id
			and i.index_id = ic.index_id
	inner join sys.columns c
		on ic.object_id = c.object_id
			and ic.column_id = c.column_id
	where hs.object_id = OBJECT_ID('dbo.SalesOrder')
	group by 
			hs.object_id,
			i.name,
			hs.total_bucket_count,
			hs.empty_bucket_count,
			hs.avg_chain_length,
			hs.max_chain_length,
			i.is_primary_key,
			i.is_unique
	GO

	--> fazendo as devidas correções
	alter table dbo.SalesOrder
		drop index IX_Status;
	GO

	-- verificando o valor ideal para o BUCKET_COUNT (baseado no número de registros atuais)
	select
	  POWER( 2, CEILING( LOG( COUNT(*)) / LOG( 2))) as 'BUCKET_COUNT atual',
	  POWER( 2, CEILING( LOG( COUNT(*)) / LOG( 2)) + 1) as 'BUCKET_COUNT futuro'
	from (select distinct OrderSequence from dbo.SalesOrder) as d; -- indicando a tabela e as colunas do índice
	GO

	alter table dbo.SalesOrder
		alter index IX_OrderSequence rebuild WITH ( BUCKET_COUNT = 262144);
	GO

	-- ver novamente as estatísticas e verificando a nova cadeia de encadeamento (BUCKET CHAIN)
	GO

	-- limpando o ambiente
	drop table dbo.SalesOrder
	GO



--> Natively Compiled Stored Procedures
----------------------------------------

	--> Criando uma NCSP para retornar uma lista de temperaturas
	--> a ser exportado para um arquivo CSV
	create or alter procedure dbo.usp_VehicleTemperatureCSV
		(
			@VehicleTemperatureID_min bigint,
			@VehicleTemperatureID_max bigint
		)
	with native_compilation, schemabinding
	as
	begin atomic
		with (transaction isolation level = snapshot, language = N'English')

		select
			STRING_AGG(	Temperature, ',' ) AS csv
		from Warehouse.VehicleTemperatures
		where VehicleTemperatureID
				BETWEEN @VehicleTemperatureID_min 
					AND @VehicleTemperatureID_max;

	end;
	GO

	-- executa a NCSP dbo.usp_VehicleTemperatureCSV
	EXEC dbo.usp_VehicleTemperatureCSV 65190, 65200;
	GO

	EXEC dbo.usp_VehicleTemperatureCSV
				@VehicleTemperatureID_min = 65250,
				@VehicleTemperatureID_max = 65280;
	GO


	--> habilitando as estatisticas de NCSP
	EXEC sys.sp_xtp_control_proc_exec_stats @new_collection_value = 1
	GO
	
	select OBJECT_NAME(object_id), * from sys.dm_exec_procedure_stats where database_id = DB_ID();
	GO
	
	--> Criando uma Natively Compiled Scalar Function
	create function dbo.DateDiffDays (@Date date)
	returns int
	with native_compilation, schemabinding
	as
	begin atomic
		with (transaction isolation level = snapshot, language = N'English')

		return DATEDIFF(day, GETDATE(), @Date);

	end;
	GO

	select dbo.DateDiffDays('2015-09-09');
	GO

--> JSON
	--> Uso de funções JSON
	select
			PersonID,
			FullName,
			UserPreferences,
			JSON_VALUE(UserPreferences, N'$.theme'),
			CustomFields,
			JSON_QUERY(CustomFields,N'$.OtherLanguages')
	from Application.People
	where
			IsPermittedToLogon = 1
		and IsEmployee = 1
		and ISJSON(UserPreferences) = 1 ;
	GO

	--> Teste de JSON em In-Memory Table

	-- Preparando o ambiente
	create table dbo.OnDisk_Usuarios
	(
		ID int identity(1,1) primary key clustered,
		PessoaID int not null,
		Logon varchar(50) not null,
		Senha varbinary(32) null,
		Preferencias nvarchar(150) not null check ( ISJSON(Preferencias) = 1 ), -- JSON
		Tema as JSON_VALUE(Preferencias, '$.Tema')
	)
	GO

	create table dbo.InMemory_Usuarios
		(
			ID int identity(1,1) primary key nonclustered hash with (BUCKET_COUNT=4000),
			PessoaID int not null,
			Logon varchar(50) not null,
			Senha varbinary(32) null,
			Preferencias nvarchar(150) not null check ( ISJSON(Preferencias) = 1 ), -- JSON
			Tema as JSON_VALUE(Preferencias, '$.Tema')
		)
	with ( MEMORY_OPTIMIZED = ON,  DURABILITY = SCHEMA_AND_DATA);
	GO


	declare @js nvarchar(MAX) = 
		(
			select
					PersonID as ID,
					FullName as NomeCompleto,
					ISNULL(JSON_VALUE(UserPreferences, N'$.theme'), 'default') as Tema,
					JSON_QUERY(CustomFields,N'$.OtherLanguages') as Linguas
			from Application.People
			where
					PersonID > 1

			for json auto 
		);

	insert into dbo.OnDisk_Usuarios ( PessoaID, Logon, Senha, Preferencias )
	select
			p.PersonID as ID,
			p.LogonName as Logon,
			p.HashedPassword as Senha,
			js.Value as Preferencias
	from openjson(@js) as js
	inner join Application.People p
		on JSON_VALUE(js.Value, N'$.ID') = p.PersonID;

	insert into dbo.InMemory_Usuarios ( PessoaID, Logon, Senha, Preferencias )
	select
			p.PersonID as ID,
			p.LogonName as Logon,
			p.HashedPassword as Senha,
			js.Value as Preferencias
	from openjson(@js) as js
	inner join Application.People p
		on JSON_VALUE(js.Value, N'$.ID') = p.PersonID;
	GO 5


	-- consultando as tabelas (marcar para exibir o plano de execução)
	-- ex: 1
	select *
	from dbo.OnDisk_Usuarios
	where Tema <> 'default';
	GO
	select *
	from dbo.InMemory_Usuarios
	where Tema <> 'default';
	GO

	-- ex: 2
	select u.*
	from dbo.OnDisk_Usuarios u
	cross apply
				(
					select *
					from openjson (u.Preferencias, '$.Linguas')
				) as l

	where l.Value = 'Greek' ;
	GO

	select u.*
	from dbo.InMemory_Usuarios u
	cross apply
				(
					select *
					from openjson (u.Preferencias, '$.Linguas')
				) as l

	where l.Value = 'Greek'	;
	GO

	-- Teste sem JSON, usando LIKE
	select u.*
	from dbo.OnDisk_Usuarios u
	where Preferencias like '%Dutch%'

	select u.*
	from dbo.InMemory_Usuarios u
	where Preferencias like '%Dutch%'
	GO

	-- limpar ambiente
	drop table dbo.OnDisk_Usuarios;
	drop table dbo.InMemory_Usuarios;
	GO
