--CREATE PROCEDURE ETL.TableComparison (@SourceServerName VARCHAR(MAX), @SourceDBName VARCHAR(MAX), @SourceSchemaName VARCHAR(MAX), @SourceTableName VARCHAR(MAX),@DestinationServerName VARCHAR(MAX), @DestinationDBName VARCHAR(MAX), @DestinationSchemaName VARCHAR(MAX), @DestinationTableName VARCHAR(MAX))

--AS

	DECLARE	@LogNotes			NVARCHAR(MAX)
		,	@ValidationPassed	INT = 0
		,	@SQLString			NVARCHAR(MAX)
		,	@ParamDefinition	NVARCHAR(MAX)
		,	@TargetTableError	NVARCHAR(MAX)

--/*	Testing
		,	@SourceServerName			VARCHAR(MAX)
		,	@SourceDBName				VARCHAR(MAX)
		,	@SourceSchemaName			VARCHAR(MAX)
		,	@SourceTableName			VARCHAR(MAX)
		,	@DestinationServerName		VARCHAR(MAX)
		,	@DestinationDBName			VARCHAR(MAX)
		,	@DestinationSchemaName		VARCHAR(MAX)
		,	@DestinationTableName		VARCHAR(MAX)
			
	SET		@SourceServerName		= 'CMDB'
	SET		@SourceDBName			= 'CareManagement'
	SET		@SourceSchemaName		= 'dbo'
	SET		@SourceTableName		= 'Episodes'
	SET		@DestinationServerName	= 'P10-DWDB-03'
	SET		@DestinationDBName		= 'DWStaging'--'DWUser'
	SET		@DestinationSchemaName	= 'dbo'--'TST'
	SET		@DestinationTableName	= 'EpisodesRaw'
--*/

	SET		@ParamDefinition		=	N'	@TargetTableErrorOUT VARCHAR(MAX) OUTPUT'
	SET		@SQLString				=	N'	DECLARE @CurrentColumn VARCHAR(MAX)
	DECLARE @MissingSourceColumns VARCHAR(MAX)
	DECLARE @MissingTargetColumns VARCHAR(MAX)

	DECLARE SourceColumnValidation Cursor FOR
			SELECT c.name
			FROM ['+@SourceServerName+'].['+@SourceDBName+'].sys.tables as t
			JOIN ['+@SourceServerName+'].['+@SourceDBName+'].sys.columns as c
			ON c.object_id = t.object_id
			WHERE t.[NAME] = '''+@SourceTableName+'''
			EXCEPT
			SELECT c.name
			FROM ['+@DestinationServerName+'].['+@DestinationDBName+'].sys.tables as t
			JOIN ['+@DestinationServerName+'].['+@DestinationDBName+'].sys.columns as c
			ON c.object_id = t.object_id
			WHERE t.[NAME] = '''+@DestinationTableName+'''

	OPEN SourceColumnValidation
	FETCH NEXT FROM SourceColumnValidation INTO @CurrentColumn

	WHILE @@FETCH_STATUS = 0
	
		BEGIN 
			SET @MissingSourceColumns = CONCAT(@MissingSourceColumns,@CurrentColumn,'','')
			FETCH NEXT FROM SourceColumnValidation INTO @CurrentColumn
		END 

	CLOSE  SourceColumnValidation
	DEALLOCATE  SourceColumnValidation

	DECLARE DestinationColumnValidation Cursor FOR
			SELECT c.name
			FROM ['+@DestinationServerName+'].['+@DestinationDBName+'].sys.tables as t
			JOIN ['+@DestinationServerName+'].['+@DestinationDBName+'].sys.columns as c
			ON c.object_id = t.object_id
			WHERE t.[NAME] = '''+@DestinationTableName+'''
			EXCEPT
			SELECT c.name
			FROM ['+@SourceServerName+'].['+@SourceDBName+'].sys.tables as t
			JOIN ['+@SourceServerName+'].['+@SourceDBName+'].sys.columns as c
			ON c.object_id = t.object_id
			WHERE t.[NAME] = '''+@SourceTableName+'''

	OPEN DestinationColumnValidation
	FETCH NEXT FROM DestinationColumnValidation INTO @CurrentColumn

	WHILE @@FETCH_STATUS = 0
	
		BEGIN 
			SET @MissingTargetColumns = CONCAT(@MissingTargetColumns,@CurrentColumn,'','')
			FETCH NEXT FROM DestinationColumnValidation INTO @CurrentColumn
		END 

	CLOSE  DestinationColumnValidation
	DEALLOCATE  DestinationColumnValidation
	
	SET @MissingSourceColumns = LEFT(@MissingSourceColumns,LEN(@MissingSourceColumns)-1)	
	SET @MissingTargetColumns = LEFT(@MissingTargetColumns,LEN(@MissingTargetColumns)-1)

	IF @MissingSourceColumns	IS NOT NULL 
		BEGIN
			SET @TargetTableErrorOUT =''The source table '+@SourceTableName+' does not contain the key column(s) ''+@MissingSourceColumns
		END 
	IF @MissingTargetColumns	IS NOT NULL
		BEGIN
			SET @TargetTableErrorOUT = @TargetTableErrorOUT + ''
The target table '+@DestinationTableName+' does not contain the key column(s) ''+@MissingTargetColumns
				END'

	--PRINT @SQLString

	EXECUTE sp_executesql 
		@SQLString, 
		@ParamDefinition, 
		@TargetTableErrorOUT=@TargetTableError OUTPUT

PRINT @TargetTableError
		
--SELECT c.name AS ColumnName,TY.name AS ColumnType, C.max_length AS ColumnMaxLength, C.Precision AS ColumnPrecision
--FROM CMDB.Caremanagement.sys.tables as t
--JOIN CMDB.Caremanagement.sys.columns as c
--ON c.object_id = t.object_id
--JOIN CMDB.Caremanagement.sys.types	AS TY
--ON	C.system_type_id = TY.system_type_id
--WHERE t.[NAME] = 'Episodes'

--EXCEPT

--SELECT c.name AS ColumnName,TY.name AS ColumnType, C.max_length AS ColumnMaxLength, C.Precision AS ColumnPrecision
--FROM DWStaging.sys.tables as t
--JOIN DWStaging.sys.columns as c
--ON c.object_id = t.object_id
--JOIN DWStaging.sys.types	AS TY
--ON	C.system_type_id = TY.system_type_id
--WHERE t.[NAME] = 'EpisodesRaw'