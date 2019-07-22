ALTER PROCEDURE JerryScriptBuilder (@SourceTable VARCHAR(MAX), @TargetTable VARCHAR(MAX), @JoinKeysRaw VARCHAR(MAX))

AS

	/*	For Testing
	DECLARE @SourceTable VARCHAR(MAX)
	DECLARE @TargetTable VARCHAR(MAX)
	DECLARE @JoinKeysRaw VARCHAR(MAX)
	SET @SourceTable = 'YourSourceDB.dbo.YourSourceTable'
	SET @TargetTable = 'YourTargetDB.dbo.YourTargetTable'  
	SET @JoinKeysRaw =  'ID'
	--*/

	
/************************************************************************************
 *****							Setup all variables								*****
 ************************************************************************************/


 --	Variables for parsing the input strings
	DECLARE @SourceServer				VARCHAR(MAX)
	DECLARE @SourceDB					VARCHAR(MAX)
	DECLARE @SourceSchema				VARCHAR(MAX)
	DECLARE @SourceTableName			VARCHAR(MAX)
	DECLARE @TargetServer				VARCHAR(MAX)
	DECLARE @TargetDB					VARCHAR(MAX)
	DECLARE @TargetSchema				VARCHAR(MAX)
	DECLARE @TargetTableName			VARCHAR(MAX)
	DECLARE @JoinKeys					VARCHAR(MAX)
	DECLARE @CurrentKey					VARCHAR(MAX)
	DECLARE @SourceKeys					VARCHAR(MAX)

--	Variables for the Dynamic Column Builder Cursor
	DECLARE @SQLString					NVARCHAR(MAX)
	DECLARE @ParamDefinition			NVARCHAR(MAX)
	DECLARE @SQLColumnUpdatePredicate	VARCHAR(MAX)
	DECLARE @SQLColumnList				VARCHAR(MAX)
	DECLARE @SQLColumnValues			VARCHAR(MAX)
	DECLARE @SQLColumnUpdate			VARCHAR(MAX)

--	Additional Variables Used to populate the generated Stored Procedure
	DECLARE @ProcedureName				VARCHAR(MAX)
	DECLARE @UserName					VARCHAR(MAX)
	DECLARE @Today						VARCHAR(MAX)

--	Variables for Stored Procedure Output
	DECLARE @SQLFlowerBox				VARCHAR(MAX)
	DECLARE @SQLStartLog				VARCHAR(MAX)
	DECLARE @SQLUpdateString			VARCHAR(MAX)
	DECLARE @SQLUpdateLog				VARCHAR(MAX)
	DECLARE @SQLInsertString			VARCHAR(MAX)
	DECLARE @SQLInsertLog				VARCHAR(MAX)
	DECLARE @SQLCompletionLog			VARCHAR(MAX)

--	Variables for Error Logging
	DECLARE @SourceTableError			VARCHAR(MAX)
	DECLARE @TargetTableError			VARCHAR(MAX)
	DECLARE @JoinKeyError				VARCHAR(MAX)


/************************************************************************************
 *****					Step 1 Populate Additional Variables					*****
 ************************************************************************************/

	SET @UserName = (SELECT REPLACE(SYSTEM_USER,'YOURORG\',''))
	SET @Today = CAST(GETDATE() AS DATE)

/************************************************************************************
 ***** Step 2 Parse the input argument for @SourceTable and validate the string *****
 ************************************************************************************/

	IF	(SELECT MAX(ID) FROM SplitString(@SourceTable,'.')) = 4
		BEGIN
			SET @SourceServer		= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 1)
			SET @SourceDB			= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 2)--'YourSourceDB'
			SET @SourceSchema		= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 3)--'dbo'
			SET @SourceTableName	= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 4)--'YourSourceTable'
		END 
	ELSE IF (SELECT MAX(ID) FROM SplitString(@SourceTable,'.')) = 3
		BEGIN 
			SET @SourceDB			= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 1)--'YourSourceDB'
			SET @SourceSchema		= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 2)--'dbo'
			SET @SourceTableName	= (SELECT [DATA] FROM SplitString(@SourceTable,'.') WHERE ID = 3)--'YourSourceTable'
		END 
	ELSE 
		BEGIN
			SET @SourceTableError	= CONCAT('The Input String ''',@SourceTable,''' Is not a valid source table. This first input must be a string that references a table with a three or four part name. Ex "MyDatabase.MySchema.MyTable" or "MyServer.MyDatabase.MySchema.MyTable"')
		END

	IF @SourceServer IS NOT NULL 
		BEGIN 
			IF @SourceServer NOT IN (SELECT [name] FROM sys.servers WHERE is_linked = 1 UNION SELECT @@SERVERNAME)
				BEGIN
					SET @SourceTableError = CONCAT('The Server ',@SourceServer,' is not listed as an available server in this instance.')
				END
		END

	IF @SourceDB IS NOT NULL AND @SourceTableError IS NULL
		BEGIN	
				SET @ParamDefinition	=	N'	@SourceTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@SourceDB+''' IN ( SELECT [name] FROM ['+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+'].Master.sys.databases )THEN 1 ELSE 0 END) = 0
												BEGIN
													SET @SourceTableErrorOUT = CONCAT(''The Database '','''+@SourceDB+''','' is not listed as available on the server '','''+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+''',''.'')
												END'
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@SourceTableErrorOUT=@SourceTableError OUTPUT
		END

	IF @SourceSchema IS NOT NULL AND @SourceTableError IS NULL
		BEGIN 
				SET @ParamDefinition	=	N'	@SourceTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@SourceSchema+''' IN ( SELECT [name] FROM ['+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+'].['+@SourceDB+'].sys.schemas )THEN 1 ELSE 0 END) = 0
												BEGIN
													 SET @SourceTableErrorOUT = CONCAT(''The Schema '','''+@SourceSchema+''','' is not listed as available for '','''+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+''',''.'','''+@SourceDB+''',''.'')
												END' 
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@SourceTableErrorOUT=@SourceTableError OUTPUT
		END

	IF @SourceTable IS NOT NULL AND @SourceTableError IS NULL
		BEGIN 
				SET @ParamDefinition	=	N'	@SourceTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@SourceTableName+''' IN ( SELECT [name] FROM ['+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+'].['+@SourceDB+'].sys.tables )THEN 1 ELSE 0 END) = 0
												BEGIN
													 SET @SourceTableErrorOUT = CONCAT(''The Schema '','''+@SourceTableName+''','' is not listed as available for '','''+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @SourceServer END+''',''.'','''+@SourceDB+''',''.'','''+@SourceSchema+''')
												END' 
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@SourceTableErrorOUT=@SourceTableError OUTPUT
		END

/************************************************************************************
 ***** Step 3 Parse the input argument for @TargetTable and validate the string *****
 ************************************************************************************/

	IF	(SELECT MAX(ID) FROM SplitString(@TargetTable,'.')) = 4
		BEGIN 
			SET @TargetServer		= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 1)
			SET @TargetDB			= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 2)--'YourSourceDB'
			SET @TargetSchema		= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 3)--'dbo'
			SET @TargetTableName	= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 4)--'YourSourceTable'
		END 
	ELSE IF (SELECT MAX(ID) FROM SplitString(@TargetTable,'.')) = 3
		BEGIN 
			SET @TargetDB			= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 1)--'YourSourceDB'
			SET @TargetSchema		= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 2)--'dbo'
			SET @TargetTableName	= (SELECT [DATA] FROM SplitString(@TargetTable,'.') WHERE ID = 3)--'YourSourceTable'
		END 
	ELSE 
		BEGIN
			SET @TargetTableError	= CONCAT('The Input String ''',@TargetTable,''' Is not a valid target table. This first input must be a string that references a table with a three or four part name. Ex "MyDatabase.MySchema.MyTable" or "MyServer.MyDatabase.MySchema.MyTable"')
		END

	IF @TargetServer IS NOT NULL 
		BEGIN 
			IF @TargetServer NOT IN (SELECT [name] FROM sys.servers WHERE is_linked = 1 UNION SELECT @@SERVERNAME)
				BEGIN
					SET @TargetTableError = CONCAT('The Server ',@TargetServer,' is not listed as an available server in this instance.')
				END
		END

	IF @TargetDB IS NOT NULL AND @TargetTableError IS NULL
		BEGIN	
				SET @ParamDefinition	=	N'	@TargetTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@TargetDB+''' IN ( SELECT [name] FROM ['+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].Master.sys.databases )THEN 1 ELSE 0 END) = 0
												BEGIN
													SET @TargetTableErrorOUT = CONCAT(''The Database '','''+@TargetDB+''','' is not listed as available on the server '','''+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+''',''.'')
												END'
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@TargetTableErrorOUT=@TargetTableError OUTPUT
		END

	IF @TargetSchema IS NOT NULL AND @TargetTableError IS NULL
		BEGIN 
				SET @ParamDefinition	=	N'	@TargetTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@TargetSchema+''' IN ( SELECT [name] FROM ['+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].['+@TargetDB+'].sys.schemas )THEN 1 ELSE 0 END) = 0
												BEGIN
													 SET @TargetTableErrorOUT = CONCAT(''The Schema '','''+@TargetSchema+''','' is not listed as available for '','''+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+''',''.'','''+@TargetDB+''',''.'')
												END' 
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@TargetTableErrorOUT=@TargetTableError OUTPUT
		END

	IF @TargetTable IS NOT NULL AND @TargetTableError IS NULL
		BEGIN 
				SET @ParamDefinition	=	N'	@TargetTableErrorOUT VARCHAR(MAX) OUTPUT'
				SET @SQLString			=	N'IF (SELECT CASE WHEN '''+@TargetTableName+''' IN ( SELECT [name] FROM ['+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].['+@TargetDB+'].sys.tables )THEN 1 ELSE 0 END) = 0
												BEGIN
													 SET @TargetTableErrorOUT = CONCAT(''The Schema '','''+@TargetTableName+''','' is not listed as available for '','''+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+''',''.'','''+@TargetDB+''',''.'','''+@TargetSchema+''')
												END' 
				EXECUTE sp_executesql 
					@SQLString, 
					@ParamDefinition, 
					@TargetTableErrorOUT=@TargetTableError OUTPUT
		END

/************************************************************************************
 ***** Step 4 Parse the input argument for @JoinKeysRaw and validate the string *****
 ************************************************************************************/

	SET @ParamDefinition	=	N'	@TargetTableErrorOUT VARCHAR(MAX) OUTPUT'
	SET @SQLString			=	N'	DECLARE @CurrentKey VARCHAR(MAX)
	DECLARE @MissingSourceKeys VARCHAR(MAX)
	DECLARE @MissingTargetKeys VARCHAR(MAX)

	DECLARE SourceKeyValidation Cursor FOR
				SELECT [Data]
				FROM SplitString('''+@JoinKeysRaw+''','','')
				EXCEPT
				SELECT c.name
				FROM ['+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].'+@SourceDB+'.sys.tables as t
				JOIN ['+CASE WHEN @SourceServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].'+@SourceDB+'.sys.columns as c
				ON c.object_id = t.object_id
				WHERE t.[NAME] = '''+@SourceTableName+'''

	OPEN SourceKeyValidation
	FETCH NEXT FROM SourceKeyValidation INTO @CurrentKey

	WHILE @@FETCH_STATUS = 0
	
		BEGIN 
			SET @MissingSourceKeys = CONCAT(@MissingSourceKeys,@CurrentKey,'','')
			FETCH NEXT FROM SourceKeyValidation INTO @CurrentKey
		END 

	CLOSE  SourceKeyValidation
	DEALLOCATE  SourceKeyValidation

	DECLARE TargetKeyValidation Cursor FOR
				SELECT [Data]
				FROM SplitString('''+@JoinKeysRaw+''','','')
				EXCEPT
				SELECT c.name
				FROM ['+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].'+@TargetDB+'.sys.tables as t
				JOIN ['+CASE WHEN @TargetServer IS NULL THEN @@SERVERNAME ELSE @TargetServer END+'].'+@TargetDB+'.sys.columns as c
				ON c.object_id = t.object_id
				WHERE t.[NAME] = '''+@TargetTableName+'''

	OPEN TargetKeyValidation
	FETCH NEXT FROM TargetKeyValidation INTO @CurrentKey

	WHILE @@FETCH_STATUS = 0
	
		BEGIN 
			SET @MissingTargetKeys = CONCAT(@MissingTargetKeys,@CurrentKey,'','')
			FETCH NEXT FROM TargetKeyValidation INTO @CurrentKey
		END 

	CLOSE  TargetKeyValidation
	DEALLOCATE  TargetKeyValidation
	
	SET @MissingSourceKeys = LEFT(@MissingSourceKeys,LEN(@MissingSourceKeys)-1)	
	SET @MissingTargetKeys = LEFT(@MissingTargetKeys,LEN(@MissingTargetKeys)-1)

	IF @MissingSourceKeys	IS NOT NULL 
		BEGIN
			SET @TargetTableErrorOUT =''The source table '+@SourceTable+' does not contain the key column(s) ''+@MissingSourceKeys
		END 
	IF @MissingTargetKeys	IS NOT NULL
		BEGIN
			SET @TargetTableErrorOUT = @TargetTableErrorOUT + ''
The target table '+@TargetTable+' does not contain the key column(s) ''+@MissingTargetKeys
				END'

	EXECUTE sp_executesql 
		@SQLString, 
		@ParamDefinition, 
		@TargetTableErrorOUT=@TargetTableError OUTPUT

	IF @JoinKeyError IS NOT NULL 
		BEGIN
			GOTO ErrorCheck
		END 
					
	DECLARE DynamicTargetKeys CURSOR FOR

		SELECT [Data]
		FROM SplitString(@JoinKeysRaw,',')

		OPEN DynamicTargetKeys
		FETCH NEXT FROM DynamicTargetKeys INTO @CurrentKey

		WHILE @@FETCH_STATUS = 0
	
		BEGIN 
		
			SET @JoinKeys = CONCAT(@JoinKeys,'src.',@CurrentKey,' = tgt.',@CurrentKey,'
			AND ')
			FETCH NEXT FROM DynamicTargetKeys INTO @CurrentKey
		
		END 

	CLOSE  DynamicTargetKeys
	DEALLOCATE  DynamicTargetKeys

	SET @JoinKeys = LEFT(@JoinKeys,LEN(@JoinKeys)-7)

/************************************************************************************
 *****	Step 5 If errors have been discovered stop execution and print errors	*****
 ************************************************************************************/
 
 ErrorCheck:
	IF @SourceTableError IS NOT NULL OR @TargetTableError IS NOT NULL
		BEGIN
			GOTO TheEnd
		END

/************************************************************************************
 *****			Step 6 Dynamicly determine what columns exist in both tables	*****
 ************************************************************************************/
		
	SET @ProcedureName = CONCAT(@TargetTableName,'Load')	

	SET @SQLString = 

	N'
	DECLARE @CurrentColumnName AS VARCHAR(MAX)

	DECLARE DynamicColumnBuilder CURSOR FOR

		SELECT c.name
		FROM '+@SourceDB+'.sys.tables as t
		JOIN '+@SourceDB+'.sys.columns as c
		ON c.object_id = t.object_id
		WHERE t.[NAME] = '''+@SourceTableName+'''
		INTERSECT
		SELECT c.name
		FROM '+@TargetDB+'.sys.tables as t
		JOIN '+@TargetDB+'.sys.columns as c
		ON c.object_id = t.object_id
		WHERE t.[NAME] = '''+@TargetTableName+'''

		OPEN DynamicColumnBuilder
		FETCH NEXT FROM DynamicColumnBuilder INTO @CurrentColumnName

		WHILE @@FETCH_STATUS = 0
	
		BEGIN 

			SET @SQLColumnListOUT = CONCAT(@SQLColumnListOUT ,@CurrentColumnName,''
		,	'')
			SET @SQLColumnValuesOUT = CONCAT(@SQLColumnValuesOUT,''src.'',@CurrentColumnName,''
		,	'')

			FETCH NEXT FROM DynamicColumnBuilder INTO @CurrentColumnName

		END



	CLOSE  DynamicColumnBuilder
	DEALLOCATE  DynamicColumnBuilder

	DECLARE DynamicUpdateColumnBuilder CURSOR FOR

		SELECT c.name
		FROM '+@SourceDB+'.sys.tables as t
		JOIN '+@SourceDB+'.sys.columns as c
		ON c.object_id = t.object_id
		WHERE t.[NAME] = '''+@SourceTableName+'''
		INTERSECT
		SELECT c.name
		FROM '+@TargetDB+'.sys.tables as t
		JOIN '+@TargetDB+'.sys.columns as c
		ON c.object_id = t.object_id
		WHERE t.[NAME] = '''+@TargetTableName+'''
		EXCEPT
		SELECT [Data]
		FROM SplitString('''+@JoinKeysRaw+''','','')


		OPEN DynamicUpdateColumnBuilder
		FETCH NEXT FROM DynamicUpdateColumnBuilder INTO @CurrentColumnName

		WHILE @@FETCH_STATUS = 0
	
		BEGIN 

			SET @SQLColumnUpdateOUT =CONCAT(@SQLColumnUpdateOUT,''tgt.'',@CurrentColumnName,''	=	src.'',@CurrentColumnName+''
		,	'')
			SET @SQLColumnUpdatePredicateOUT =CONCAT(@SQLColumnUpdatePredicateOUT,''tgt.'',@CurrentColumnName,''	<>	src.'',@CurrentColumnName+''
		OR	'')

			FETCH NEXT FROM DynamicUpdateColumnBuilder INTO @CurrentColumnName

		END



	CLOSE  DynamicUpdateColumnBuilder
	DEALLOCATE  DynamicUpdateColumnBuilder

	SET @SQLColumnListOUT = LEFT(@SQLColumnListOUT,LEN(@SQLColumnListOUT)-5)
	SET @SQLColumnValuesOUT = LEFT(@SQLColumnValuesOUT,LEN(@SQLColumnValuesOUT)-5)
	SET @SQLColumnUpdateOUT = LEFT(@SQLColumnUpdateOUT,LEN(@SQLColumnUpdateOUT)-5)
	SET @SQLColumnUpdatePredicateOUT = LEFT(@SQLColumnUpdatePredicateOUT,LEN(@SQLColumnUpdatePredicateOUT)-5)
	'
	--PRINT @SQLString

	SET @ParamDefinition =	N'	@SQLColumnListOUT VARCHAR(MAX) OUTPUT, 
								@SQLColumnValuesOUT VARCHAR(MAX) OUTPUT, 
								@SQLColumnUpdateOUT VARCHAR(MAX) OUTPUT,
								@SQLColumnUpdatePredicateOUT VARCHAR(MAX) OUTPUT'

	--PRINT @ParamDefinition

	EXECUTE sp_executesql 
		@SQLString, 
		@ParamDefinition, 
		@SQLColumnListOUT=@SQLColumnList OUTPUT, 
		@SQLColumnValuesOUT=@SQLColumnValues OUTPUT, 
		@SQLColumnUpdateOUT=@SQLColumnUpdate OUTPUT,
		@SQLColumnUpdatePredicateOUT=@SQLColumnUpdatePredicate OUTPUT

		

/************************************************************************************
 *****		Step 7 Create the code for the stored procedure in sections			*****
 ************************************************************************************/

	-- Create a default flower box
	SET @SQLFlowerBox = '/*****************************************************************
	Name:			'+@ProcedureName+'
	Description:	
	Parameters:		@RequestingProcessName
	Assumptions:
	Dependencies:	'+@SourceTableName+'
					'+@TargetTableName+'
					YourSourceDB.dbo.SplitString
					YourSourceDB.dbo.ProgrammabilityStatisticsLogInsert
	Return:	
	Usage:			EXEC '+@ProcedureName+' RequestingProcessName
	Summary:
	History:		Date			User Name		Action
					---------------	---------------	-------------
					'+@Today+'		'+@UserName+'	Created
 ******************************************************************/

	CREATE PROCEDURE '+@ProcedureName+' (@RequestingProcessName varchar(75))

	AS 

	BEGIN

	--Script setup
	DECLARE	@RowLoadDate INT;
	SET		@RowLoadDate = CAST(CONVERT(CHAR(8), GETDATE(), 112) AS INT);



	--Log setup
	DECLARE	@StartDateTime			DATETIME 
		,	@EndDateTime			DATETIME
		,	@RowsAffected			INT
		,	@ErrorNum				INT
		,	@DateDiff				INT
		,	@DatabaseName			VARCHAR(128)
		,	@ProgrammabilityObject	VARCHAR(128)
		,	@Message				VARCHAR(100)
		,	@Body					NVARCHAR(100)
		,	@DefaultMissingDate		DATE

	SELECT	@DatabaseName			=	DB_Name()
		,	@ProgrammabilityObject	=	@@Procid
		,	@StartDateTime			=	GETDATE()
		,	@DefaultMissingDate		=	''01/01/1900'''


	SET @SQLStartLog ='
	SELECT	@RowsAffected = (SELECT COUNT(0) FROM  ' +@SourceDB+ '.' +@SourceSchema+ '.' +@SourceTableName+ ')
		,	@Message = ''Begin Process '+@ProcedureName+', Incoming Rows: ''+ LTRIM(STR(@RowsAffected))
		,	@EndDateTime = GETDATE()
		,	@DateDiff = DATEDIFF(ms, @StartDateTime, @EndDateTime)
	EXEC	YourSourceDB.dbo.ProgrammabilityStatisticsLogInsert	@DatabaseName			=	@DatabaseName 
															,	@ProgrammabilityObject	=	@ProgrammabilityObject
															,	@RequestingProcessName	=	@RequestingProcessName
															,	@ElapsedTime			=	@DateDiff
															,	@RowsAffected			=	0
															,	@ProcessReturnCode		=	0
															,	@ProcessReturnMessage	=	@Message
															  '

	SET @SQLUpdateString ='
	UPDATE	tgt
	SET		' +@SQLColumnUpdate+ '
	FROM		' +@SourceDB+ '.' +@SourceSchema+ '.' +@SourceTableName+ ' AS src
	LEFT JOIN	' +@TargetDB+ '.' +@TargetSchema+ '.' +@TargetTableName+ ' AS tgt
		ON		' +@JoinKeys+'
	WHERE	'+@SQLColumnUpdatePredicate+''


	SET @SQLUpdateLog ='
	SELECT	@ErrorNum = @@ERROR
		,	@RowsAffected = @@ROWCOUNT
		,	@Message = ''Rows Updated for '+@ProcedureName+'''+ LTRIM(STR(@RowsAffected))
		,	@EndDateTime = GETDATE()
		,	@DateDiff = DATEDIFF(ms, @StartDateTime, @EndDateTime)
	EXEC	YourSourceDB.dbo.ProgrammabilityStatisticsLogInsert	@DatabaseName			=	@DatabaseName
															,	@ProgrammabilityObject	=	@ProgrammabilityObject
															,	@RequestingProcessName	=	@RequestingProcessName
															,	@ElapsedTime			=	@DateDiff
															,	@RowsAffected			=	@RowsAffected
															,	@ProcessReturnCode		=	@ErrorNum
															,	@ProcessReturnMessage	=	@Message
															  '


	SET @SQLInsertString = '	INSERT INTO ' +@TargetDB+ '.' +@TargetSchema+ '.' +@TargetTableName+ '
		(	' +@SQLColumnList+ '		,	RowLoadDateTime
		)
	SELECT	'+@SQLColumnValues+ '		,	RowLoadDate	=	GETDATE()
	FROM	' +@SourceDB+ '.' +@SourceSchema+ '.' +@SourceTableName+ ' AS src
	WHERE NOT EXISTS 
		(
			SELECT	*
			FROM	' +@TargetDB+ '.' +@TargetSchema+ '.' +@TargetTableName+ ' AS tgt
			WHERE	' +@JoinKeys+'
		)'


	SET @SQLInsertLog ='
	SELECT	@ErrorNum = @@ERROR
		,	@RowsAffected = @@ROWCOUNT
		,	@Message = ''Rows Inserted for '+@ProcedureName+'''+ LTRIM(STR(@RowsAffected))
		,	@EndDateTime = GETDATE()
		,	@DateDiff = DATEDIFF(ms, @StartDateTime, @EndDateTime)
	EXEC	YourSourceDB.dbo.ProgrammabilityStatisticsLogInsert	@DatabaseName			=	@DatabaseName
															,	@ProgrammabilityObject	=	@ProgrammabilityObject
															,	@RequestingProcessName	=	@RequestingProcessName
															,	@ElapsedTime			=	@DateDiff
															,	@RowsAffected			=	@RowsAffected
															,	@ProcessReturnCode		=	@ErrorNum
															,	@ProcessReturnMessage	=	@Message
															  '

	SET @SQLCompletionLog ='
	SELECT	@ErrorNum = @@ERROR
		,	@RowsAffected = @@ROWCOUNT
		,	@Message = ''ODS Load Complete for '+@ProcedureName+'''+ LTRIM(STR(@RowsAffected))
		,	@EndDateTime = GETDATE()
		,	@DateDiff = DATEDIFF(ms, @StartDateTime, @EndDateTime)
	EXEC	YourSourceDB.dbo.ProgrammabilityStatisticsLogInsert	@DatabaseName			=	@DatabaseName
															,	@ProgrammabilityObject	=	@ProgrammabilityObject
															,	@RequestingProcessName	=	@RequestingProcessName
															,	@ElapsedTime			=	@DateDiff
															,	@RowsAffected			=	@RowsAffected
															,	@ProcessReturnCode		=	@ErrorNum
															,	@ProcessReturnMessage	=	@Message	  
	END
	GO'

/************************************************************************************
 *****					Step 8 Output the final stored procedure				*****
 ************************************************************************************/

	PRINT @SQLFlowerBox

	PRINT @SQLStartLog

	PRINT @SQLUpdateString

	PRINT @SQLUpdateLog

	PRINT @SQLInsertString
	
	PRINT @SQLInsertLog	

	PRINT @SQLCompletionLog

/************************************************************************************
 *****							Step 9 Output errors							*****
 ************************************************************************************/

	TheEnd:
	
	IF @SourceTableError IS NOT NULL OR @TargetTableError IS NOT NULL OR @JoinKeyError IS NOT NULL
	BEGIN
		PRINT	@SourceTableError
		PRINT	@TargetTableError
		PRINT	@JoinKeyError
	END