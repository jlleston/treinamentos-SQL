------------------------------------------------------------------------
------[              MODULO 4 - Índices COLUMNSTORE             ]-------
------   criando tabelas In-Memory com NONCLUSTERED COLUMNSTORE  -------
------------------------------------------------------------------------

	use [WideWorldImportersDW]
	GO

	set nocount on;
	GO


	--> Vamos comparar três tabelas para validar a performance
	-->		1ª - Tabela Disk-Based com índice CLUSTER ROWSTORE
	-->		2ª - Tabela Disk-Based com índice CLUSTER COLUNSTORE
	-->		3ª - Tabela In-Memory com índice CLUSTER COLUNSTORE
	------------------------------------------------------------
	drop table if exists dbo.EmployeesWithClusteredIndex;
	drop table if exists dbo.EmployeesWithClusteredColumnstoreIndex;
	drop table if exists dbo.EmployeesWithInMemoryClusteredColumnstoreIndex;
	GO

	-- Criando a tabela Disk-Based com índice CLUSTER ROWSTORE
	create table dbo.EmployeesWithClusteredIndex
		(
			EmpID int NOT NULL,
			EmpName varchar(50) NOT NULL,
			EmpAddress varchar(50) NOT NULL,
			EmpDEPID int NOT NULL,
			EmpBirthDay datetime NULL,
			PRIMARY KEY CLUSTERED (	EmpID ASC )
		);
	GO

	-- Criando a tabela Disk-Based com índice CLUSTER COLUNSTORE
	create table dbo.EmployeesWithClusteredColumnstoreIndex
		(
			EmpID int NOT NULL,
			EmpName varchar(50) NOT NULL,
			EmpAddress varchar(50) NOT NULL,
			EmpDEPID int NOT NULL,
			EmpBirthDay datetime NULL,
			INDEX Employees_CCI CLUSTERED COLUMNSTORE WITH (COMPRESSION_DELAY = 30)
		);
	GO

	-- Criando a tabela In-Memory com índice CLUSTER COLUNSTORE
	create table dbo.EmployeesWithInMemoryClusteredColumnstoreIndex
		(
			EmpID int NOT NULL constraint PK_Employees_EmpID
									primary key nonclustered hash (EmpID) with (bucket_count = 100000),
			EmpName varchar(50) NOT NULL,
			EmpAddress varchar(50) NOT NULL,
			EmpDEPID int NOT NULL,
			EmpBirthDay datetime NULL,
			INDEX Employees_IMCCI CLUSTERED COLUMNSTORE WITH (COMPRESSION_DELAY = 0)
		) with (memory_optimized = on, durability = SCHEMA_AND_DATA);
	GO


	-- fazendo a carga simultânea das três tabelas com os mesmos valores
	declare @cont int = 0;
	declare @EmpID int,
			@EmpName varchar(50),
			@EmpAddress varchar(50),
			@EmpDEPID int,
			@EmpBirthDay datetime;

	begin tran;

	while @cont < 10000
	begin
		
		set @EmpID = @cont + 1;
		set @EmpName =  (select [value]
						 from (
								select ROW_NUMBER() over (order by value) as ordem, [value]
								from STRING_SPLIT (N'João,Maria,Lucas,Ana,Manoel,Fabricio,Joana,Pedro,Beatriz,Caio,Heloisa', N',')
							) as n
						 where ordem = (@cont % 11 + 1)
						) + ' ' + ISNULL(
						(select [value]
						 from (
								select ROW_NUMBER() over (order by value) as ordem, value
								from STRING_SPLIT (N'Silva,Mendes,Costa,Almeida,Sá,Netto,Cruz,Camon,Bento,Souza,Vaz', N',')
							) as n
						 where ordem = cast((@cont % 11 + 1) * RAND() as int) % 12
						),'do Amaral');

		set @EmpDEPID = RAND() * (@cont % 10000);
		set @EmpAddress = CONCAT('Rua ',@EmpName,', número ', @EmpDEPID);
--		select (1930 + (@cont % 100)), (12-(@cont % 12)), (@cont % 28);
		set @EmpBirthDay = DATEFROMPARTS(1930 + (@cont % 100), 12-(@cont % 12), 1 + (@cont % 28));
		
		insert into dbo.EmployeesWithClusteredIndex
				(EmpID, EmpName, EmpAddress, EmpDEPID, EmpBirthDay)
			values (@EmpID, @EmpName, @EmpAddress, @EmpDEPID, @EmpBirthDay);

		insert into dbo.EmployeesWithClusteredColumnstoreIndex
				(EmpID, EmpName, EmpAddress, EmpDEPID, EmpBirthDay)
			values (@EmpID, @EmpName, @EmpAddress, @EmpDEPID, @EmpBirthDay);

		insert into dbo.EmployeesWithInMemoryClusteredColumnstoreIndex
				(EmpID, EmpName, EmpAddress, EmpDEPID, EmpBirthDay)
			values (@EmpID, @EmpName, @EmpAddress, @EmpDEPID, @EmpBirthDay);

		set @cont = @cont + 1;
	end;
	commit;
	GO

	-- iniciando o teste de performance, sempre zerando os caches antes de cada execução
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS ;
	GO

	set statistics time on;
	set statistics io on;
	GO

	select EmpName, EmpAddress,EmpBirthDay 
	from EmployeesWithClusteredIndex 
	where EmpID> 174 AND EmpName LIKE '%Ana%';
	GO

	set statistics time off;
	set statistics io off;
	GO

	CHECKPOINT;
	DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS ;
	GO

	set statistics time on;
	set statistics io on;
	GO

	select EmpName, EmpAddress,EmpBirthDay 
	from EmployeesWithClusteredColumnstoreIndex 
	where EmpID> 174 AND EmpName LIKE '%Ana%';
	GO

	set statistics time off;
	set statistics io off;
	GO

	CHECKPOINT;
	DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS ;
	GO

	set statistics time on;
	set statistics io on;
	GO

	select EmpName, EmpAddress,EmpBirthDay 
	from EmployeesWithInMemoryClusteredColumnstoreIndex 
	where EmpID> 174 AND EmpName LIKE '%Ana%';
	GO

	set statistics time off;
	set statistics io off;
	GO

	-- analisando os rowgroups gerados pelos índices COLUMNSTORE
	select
			OBJECT_NAME(crg.[object_id]) as tabela,
			i.name as indice,
			i.type_desc,
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
	where crg.[object_id]
			IN (OBJECT_ID('dbo.EmployeesWithClusteredColumnstoreIndex'),
				OBJECT_ID('dbo.EmployeesWithInMemoryClusteredColumnstoreIndex'))
	order by tabela, indice, crg.row_group_id;
	GO

	-- analisando os dados do índice CLUSTERED COLUMNSTORE na tabela In-Memory
	select
		i.name as [columnstore],
		moa.type_desc,
		mc.memory_consumer_type_desc,
		mc.allocated_bytes / 1024 as [allocated_kb],
		mc.used_bytes / 1024 as [used_kb]
	from sys.memory_optimized_tables_internal_attributes moa
	inner join sys.indexes i ON moa.object_id = i.object_id AND i.type in (5,6)
	inner join sys.dm_db_xtp_memory_consumers mc ON moa.xtp_object_id=mc.xtp_object_id
	inner join sys.objects o on moa.object_id=o.object_id
	where
			moa.type in (0,2,3,4)
		and moa.object_id = OBJECT_ID('dbo.EmployeesWithInMemoryClusteredColumnstoreIndex');
	GO
	
	-- limpando o ambiente
	drop table if exists dbo.EmployeesWithClusteredIndex;
	drop table if exists dbo.EmployeesWithClusteredColumnstoreIndex;
	drop table if exists dbo.EmployeesWithInMemoryClusteredColumnstoreIndex;
	GO
