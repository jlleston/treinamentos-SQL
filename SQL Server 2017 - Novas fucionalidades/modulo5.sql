------------------------------------------------------------------------
------[              MODULO 5 - Segurança de Dados              ]-------
------------------------------------------------------------------------

	--> Dynamic Data Masking

	use [WideWorldImporters]
	GO

	-- confirma que o nível de compatibilidade é o do SQL Server 2017
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
		('Janice',  'Galvão',  '62.99123.4568', '50727053211', 'JGalvin@contoso.com.co'),  
		('João',    'Cardozo', '62.99123.4578', '40227031202', 'jCardozo@contoso.com'),  
		('Elis',    'Pontes',  '61.98111.2521', '30127029729', 'e.pontes@contoso.br'),  
		('Zheng',   'Mu',      '62.99123.4111', '08827014115', 'ZMu@contoso.net');
	GO

	select * from dbo.Participantes;
	GO

	-- criando usuário para teste
	drop user if exists TestMASK;
	GO

	create user TestMASK without login;
	GO
	
	-- concedendo permissão de SELECT para o usuário de teste
	GRANT SELECT ON dbo.Participantes TO TestMASK;  
	GO

	-- simula a execução de contexto com outro usuário
	EXECUTE AS USER = 'TestMASK';
	GO

	-- verificando usuário ativo
	select
		USER_NAME() as UsuarioContexto,
		ORIGINAL_LOGIN() as UsuarioLogon;
	GO

	-- embora com permissão de ler, não tem permissão para remover a máscara durante a consulta
	select * from dbo.Participantes;
	GO

	-- retorna a execução de contexto ao usuário efetivamente logado
	REVERT;  
	GO

	-- adicionando uma máscara em uma coluna de tabela existente (neste caso o Sobrenome)
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
	
	-- adicionando máscara customizada no CPF
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

	-- corrigindo a máscara customizada no CPF
	alter table dbo.Participantes
		alter column CPF ADD MASKED WITH (FUNCTION = 'partial(2,"XXXXXX-",2)');  
	GO

	-- removendo a máscara da coluna Sobrenome
	alter table dbo.Participantes
		alter column Email DROP MASKED;
	GO

	-- verificando a remoção da máscara
	EXECUTE AS USER = 'TestMASK';
	GO

	select * from dbo.Participantes;
	GO

	-- retorna a execução de contexto ao usuário efetivamente logado
	REVERT;  
	GO

	-- concedendo a permissão UNMASK, que é necessária a todos os usuários que queiram ler os dados sem máscara
	GRANT UNMASK TO TestMASK;
	GO

	-- simula a execução de contexto com o usuário de teste
	EXECUTE AS USER = 'TestMASK';
	GO

	-- agora o usuário pode ler os dados sem restrições
	select * from dbo.Participantes;
	GO

	-- retorna a execução de contexto ao usuário efetivamente logado
	REVERT;  
	GO

	REVOKE UNMASK TO TestMASK;
	GO




	--> RLS - Row-level Security

	
	-- Criando a Função de PREDICADO
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

	-- criando os logins e usuários para demonstração
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

	-- verifica as roles de segurança desta base de dados
	select * from sys.database_principals
	where
			type = 'R' -- ROLES
		and principal_id < 16384; -- roles fixas 
	GO

	-- adiciona o usuário GreatLakesUser a role adequada
	ALTER ROLE [Great Lakes Sales] ADD MEMBER GreatLakesUser;
	GO

	SELECT * FROM Sales.Customers; -- tabela com controle de RLS, observar dados e numero de registros (663)
	GO

	-- liberar permissões para a role [Great Lakes Sales]
	GRANT SELECT ON Sales.Customers TO [Great Lakes Sales];
	GRANT UPDATE ON Sales.Customers TO [Great Lakes Sales];
	GRANT SELECT ON [Application].Cities TO [Great Lakes Sales];
	GRANT SELECT ON [Application].Countries TO [Great Lakes Sales];
	GO

	-- impersonando o usuário GreatLakesUser
	EXECUTE AS USER = 'GreatLakesUser';
	GO

	-- verificando novamente a tabela CUSTOMERS
	SELECT * FROM Sales.Customers; 
	GO

	-- analisando tabelas Countries e Customers --> Prestar atenção nos dados espaciais (Região dos Grandes Lagos)
	SELECT c.Border 
	FROM [Application].Countries AS c
	WHERE c.CountryName = N'United States'
	UNION ALL
	SELECT c.DeliveryLocation 
	FROM Sales.Customers AS c;
	GO

	-- a tentativa de fazer update nos dados fora da região de acesso são bloqueadas
	UPDATE Sales.Customers            -- Attempt to update
	SET DeliveryCityID = 3            -- este ID de cidade não pertence ao "Great Lakes Sales Territory"
	WHERE DeliveryCityID = 32887;     -- mas o cliente é desse território

	REVERT;
	GO



	-->> setando a varável de sessão SESSION_CONTEXT
	EXEC sp_set_session_context N'SalesTerritory', N'Great Lakes', @read_only = 1;
	GO

	-- Verificando os valores de SESSION_CONTEXT
	SELECT SESSION_CONTEXT(N'SalesTerritory');
	GO

