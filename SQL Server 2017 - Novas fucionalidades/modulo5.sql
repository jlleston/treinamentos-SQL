------------------------------------------------------------------------
------[              MODULO 5 - Seguran�a de Dados              ]-------
------------------------------------------------------------------------

	--> Dynamic Data Masking

	use [WideWorldImporters]
	GO

	-- confirma que o n�vel de compatibilidade � o do SQL Server 2017
	alter database [WideWorldImporters] set compatibility_level = 140
	GO 

	-- Verifica tabelas com MASKED COLUMNS
	select schema_name(schema_id) as [schema], tbl.name as table_name, c.name, c.is_masked, c.masking_function
	from sys.masked_columns as c
	JOIN sys.tables as tbl
		on c.[object_id] = tbl.[object_id]
	where c.is_masked = 1;
	GO

	-- Criando tabela para uso do Data Masking
	drop table if exists dbo.Participantes;
	GO

	create table dbo.Participantes
	(
		ParticipanteID int identity(1,1) primary key,  
		Nome nvarchar(40)								MASKED WITH (FUNCTION = 'partial(1,"XXXXXXX",0)') not null,
		SobreNome nvarchar(60) not null,
		NomeCompleto as TRIM(CONCAT(Nome,' ', Sobrenome)),
		CPF varchar(11) not null,
		Telefone varchar(15)							MASKED WITH (FUNCTION = 'default()') null,  
		Email varchar(60)								MASKED WITH (FUNCTION = 'email()') null
	);
	GO

	insert dbo.Participantes (Nome, SobreNome, Telefone, CPF, Email)
	values
		('Roberto', 'Costa',   '62.99123.4567', '60127068432', 'RTamburello@contoso.com'),  
		('Janice',  'Galv�o',  '62.99123.4568', '50727053211', 'JGalvin@contoso.com.co'),  
		('Jo�o',    'Cardozo', '62.99123.4578', '40227031202', 'jCardozo@contoso.com'),  
		('Elis',    'Pontes',  '61.98111.2521', '30127029729', 'e.pontes@contoso.br'),  
		('Zheng',   'Mu',      '62.99123.4111', '08827014115', 'ZMu@contoso.net');
	GO

	select * from dbo.Participantes;
	GO

	-- criando usu�rio para teste
	drop user if exists TestMASK;
	GO

	create user TestMASK without login;
	GO
	
	-- concedendo permiss�o de SELECT para o usu�rio de teste
	GRANT SELECT ON dbo.Participantes TO TestMASK;  
	GO

	-- simula a execu��o de contexto com outro usu�rio
	EXECUTE AS USER = 'TestMASK';
	GO

	-- verificando usu�rio ativo
	select
		USER_NAME() as UsuarioContexto,
		ORIGINAL_LOGIN() as UsuarioLogon;
	GO

	-- embora com permiss�o de ler, n�o tem permiss�o para remover a m�scara durante a consulta
	select * from dbo.Participantes;
	GO

	-- retorna a execu��o de contexto ao usu�rio efetivamente logado
	REVERT;  
	GO

	-- adicionando uma m�scara em uma coluna de tabela existente (neste caso o Sobrenome)
	alter table dbo.Participantes
		alter column Sobrenome ADD MASKED WITH (FUNCTION = 'partial(2,"XXX",1)');  
	GO

	-- quando existe dependencia nas colunas (COMPUTED, SCHEMABIND), precisamos remover a dependencia temporariamente
	begin transaction;
		alter table dbo.Participantes drop column NomeCompleto;

		alter table dbo.Participantes
			alter column Sobrenome ADD MASKED WITH (FUNCTION = 'partial(2,"XXX",1)');  

		alter table dbo.Participantes
			add NomeCompleto as TRIM(CONCAT(Nome,' ', Sobrenome));

	commit;
	GO
	
	-- adicionando m�scara customizada no CPF
	alter table dbo.Participantes
		alter column CPF ADD MASKED WITH (FUNCTION = 'partial(0,"XXXXXXXXX-XX",4)');  
	GO

	select * from dbo.Participantes;
	GO

	EXECUTE AS USER = 'TestMASK';
	GO

	select * from dbo.Participantes;
	GO

	REVERT;  
	GO

	-- corrigindo a m�scara customizada no CPF
	alter table dbo.Participantes
		alter column CPF ADD MASKED WITH (FUNCTION = 'partial(2,"XXXXXX-",2)');  
	GO

	-- removendo a m�scara da coluna Sobrenome
	alter table dbo.Participantes
		alter column Email DROP MASKED;
	GO

	-- verificando a remo��o da m�scara
	EXECUTE AS USER = 'TestMASK';
	GO

	select * from dbo.Participantes;
	GO

	-- retorna a execu��o de contexto ao usu�rio efetivamente logado
	REVERT;  
	GO

	-- concedendo a permiss�o UNMASK, que � necess�ria a todos os usu�rios que queiram ler os dados sem m�scara
	GRANT UNMASK TO TestMASK;
	GO

	-- simula a execu��o de contexto com o usu�rio de teste
	EXECUTE AS USER = 'TestMASK';
	GO

	-- agora o usu�rio pode ler os dados sem restri��es
	select * from dbo.Participantes;
	GO

	-- retorna a execu��o de contexto ao usu�rio efetivamente logado
	REVERT;  
	GO

	REVOKE UNMASK TO TestMASK;
	GO




	--> RLS - Row-level Security

	
	-- Criando a Fun��o de PREDICADO
	 CREATE OR ALTER FUNCTION [Application].DetermineCustomerAccess(@CityID int)
	 RETURNS TABLE
	 WITH SCHEMABINDING 
	 AS 
	 RETURN (SELECT 1 AS AccessResult
			 WHERE
					IS_ROLEMEMBER(N'db_owner') <> 0 

				 OR IS_ROLEMEMBER((SELECT sp.SalesTerritory 
								   FROM [Application].Cities AS c
								   INNER JOIN [Application].StateProvinces AS sp
								   ON c.StateProvinceID = sp.StateProvinceID
								   WHERE c.CityID = @CityID) + N' Sales') <> 0

 				OR (ORIGINAL_LOGIN() = N'Website' 
 					AND EXISTS (SELECT 1
 								FROM [Application].Cities AS c
 								INNER JOIN [Application].StateProvinces AS sp
 								ON c.StateProvinceID = sp.StateProvinceID
 								WHERE c.CityID = @CityID 
 								AND sp.SalesTerritory = SESSION_CONTEXT(N'SalesTerritory')))
			);
	 GO

	-- Criando a SECURITY POLICY
	CREATE SECURITY POLICY [Application].FilterCustomersBySalesTerritoryRole
		ADD FILTER PREDICATE [Application].DetermineCustomerAccess(DeliveryCityID) 
			ON Sales.Customers,
		 ADD BLOCK PREDICATE [Application].DetermineCustomerAccess(DeliveryCityID)  
			 ON Sales.Customers AFTER UPDATE
	WITH (STATE = ON);
	GO

	-- criando os logins e usu�rios para demonstra��o
	use master;
	GO

	if not exists (select 1 from sys.server_principals where name = N'GreatLakesUser')
	begin
		CREATE LOGIN GreatLakesUser 
		WITH PASSWORD = N'SQL2017!RLS',
			 CHECK_POLICY = ON,
			 CHECK_EXPIRATION = OFF,
			 DEFAULT_DATABASE = WideWorldImporters;
	end;
	GO

	if not exists (select 1 from sys.server_principals where name = N'Website')
	begin
		CREATE LOGIN Website 
		WITH PASSWORD = N'SQL2017!RLS',
			 CHECK_POLICY = ON,
			 CHECK_EXPIRATION = OFF,
			 DEFAULT_DATABASE = WideWorldImporters;
	end;
	GO

	use WideWorldImporters;
	GO

	CREATE USER GreatLakesUser FOR LOGIN GreatLakesUser;
	GO

	CREATE USER Website FOR LOGIN Website;
	GO

	-- verifica as roles de seguran�a desta base de dados
	select * from sys.database_principals
	where
			type = 'R' -- ROLES
		and principal_id < 16384; -- roles fixas 
	GO

	-- adiciona o usu�rio GreatLakesUser a role adequada
	ALTER ROLE [Great Lakes Sales] ADD MEMBER GreatLakesUser;
	GO

	SELECT * FROM Sales.Customers; -- tabela com controle de RLS, observar dados e numero de registros (663)
	GO

	-- liberar permiss�es para a role [Great Lakes Sales]
	GRANT SELECT ON Sales.Customers TO [Great Lakes Sales];
	GRANT UPDATE ON Sales.Customers TO [Great Lakes Sales];
	GRANT SELECT ON [Application].Cities TO [Great Lakes Sales];
	GRANT SELECT ON [Application].Countries TO [Great Lakes Sales];
	GO

	-- impersonando o usu�rio GreatLakesUser
	EXECUTE AS USER = 'GreatLakesUser';
	GO

	-- verificando novamente a tabela CUSTOMERS
	SELECT * FROM Sales.Customers; 
	GO

	-- analisando tabelas Countries e Customers --> Prestar aten��o nos dados espaciais (Regi�o dos Grandes Lagos)
	SELECT c.Border 
	FROM [Application].Countries AS c
	WHERE c.CountryName = N'United States'
	UNION ALL
	SELECT c.DeliveryLocation 
	FROM Sales.Customers AS c;
	GO

	-- a tentativa de fazer update nos dados fora da regi�o de acesso s�o bloqueadas
	UPDATE Sales.Customers            -- Attempt to update
	SET DeliveryCityID = 3            -- este ID de cidade n�o pertence ao "Great Lakes Sales Territory"
	WHERE DeliveryCityID = 32887;     -- mas o cliente � desse territ�rio

	REVERT;
	GO



	-->> setando a var�vel de sess�o SESSION_CONTEXT
	EXEC sp_set_session_context N'SalesTerritory', N'Great Lakes', @read_only = 1;
	GO

	-- Verificando os valores de SESSION_CONTEXT
	SELECT SESSION_CONTEXT(N'SalesTerritory');
	GO

