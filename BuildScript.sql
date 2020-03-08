DROP PROCEDURE ETL.ProcessMasterDelay;
DROP PROCEDURE ETL.ProcessMasterUpdateCompletion;
DROP PROCEDURE ETL.ProcessMasterUpdateFailure;
DROP PROCEDURE ETL.ProcessMasterAreDependenciesFailed;
DROP PROCEDURE ETL.ProcessRunLogWorkUpdate;
DROP PROCEDURE ETL.ProcessMasterIsWorkWaiting;
DROP PROCEDURE ETL.ProcessMasterSetForRun;
DROP TABLE ETL.ProcessRunLog;
DROP TABLE ETL.ProcessGroupRunLog;
DROP TABLE ETL.RunKeys;
DROP TABLE ETL.ProcessGroupProcessBridge;
DROP TABLE ETL.ProcessGroupings;
DROP TABLE ETL.ProcessDependencyBridge
DROP TABLE ETL.ProcessMaster;
DROP TABLE ETL.TestLog;
DROP SCHEMA ETL;

GO

USE DWUser;
GO

-- Isolate my project with a unique schema

CREATE SCHEMA ETL;
GO

-- Create tables for my process

CREATE TABLE ETL.ProcessMaster
	(
			ProcessId					INT	IDENTITY(1,1)	NOT NULL
		,	ProcessName					VARCHAR(50)			NOT NULL	
		,	ProcessObjectType			VARCHAR(50)			NOT NULL
		,	ProcessExecutionString		VARCHAR(MAX)		NOT NULL
		,	ProcessPriority				INT					NOT NULL
		,	IsActive					BIT					NOT NULL
		,	IncrementalLoadFieldSource	VARCHAR(30)			NULL
		,	IncrementalLoadField		VARCHAR(30)			NULL
		,	CreatedBy					VARCHAR(20)			NOT NULL
		,	CreatedDateTime				DATETIME			NOT NULL
		,	ModifiedBy					VARCHAR(20)			NULL
		,	ModifiedDateTime			DATETIME			NULL
		,	CONSTRAINT PK_ProcessId PRIMARY KEY NONCLUSTERED (ProcessId)
	);
GO

CREATE TABLE ETL.ProcessDependencyBridge
	(
			ProcessId					INT			NOT NULL
		,	DependeeProcessID			INT			NOT NULL
		,	CreatedBy					VARCHAR(20)	NOT NULL
		,	CreatedDateTime				DATETIME	NOT NULL
		,	ModifiedBy					VARCHAR(20)	NULL
		,	ModifiedDateTime			DATETIME	NULL
		,	CONSTRAINT	FK_ProcessId	FOREIGN KEY (ProcessId)
			REFERENCES	ETL.ProcessMaster (ProcessId)
			ON DELETE CASCADE
			ON UPDATE CASCADE
		,	CONSTRAINT	FK_DependeeProcessID	FOREIGN KEY (DependeeProcessID)
			REFERENCES	ETL.ProcessMaster (ProcessId)
			ON DELETE NO ACTION
			ON UPDATE NO ACTION
	);
GO

CREATE TABLE ETL.ProcessGroupings
	(
			ProcessGroupId				INT	IDENTITY(1,1)	NOT NULL
		,	ProcessGroupName			VARCHAR(50)			NOT NULL
		,	ProcessGroupFrequency		INT					NOT NULL
		,	MaxConcurrent				INT					NOT NULL
		,	CreatedBy					VARCHAR(20)			NOT NULL
		,	CreatedDateTime				DATETIME			NOT NULL
		,	ModifiedBy					VARCHAR(20)			NULL
		,	ModifiedDateTime			DATETIME			NULL
		,	CONSTRAINT PK_ProcessGroupId PRIMARY KEY NONCLUSTERED (ProcessGroupId)
	);

CREATE TABLE ETL.ProcessGroupProcessBridge
	(
			ProcessGroupId				INT					NOT NULL
		,	ProcessId					INT					NOT NULL
		,	CreatedBy					VARCHAR(20)			NOT NULL
		,	CreatedDateTime				DATETIME			NOT NULL
		,	ModifiedBy					VARCHAR(20)			NULL
		,	ModifiedDateTime			DATETIME			NULL
		,	CONSTRAINT	FK_ProcessGroupId	FOREIGN KEY (ProcessGroupId)
			REFERENCES	ETL.ProcessGroupings (ProcessGroupId)
			ON DELETE CASCADE
			ON UPDATE CASCADE
		,	CONSTRAINT	FK_ProcessId2	FOREIGN KEY (ProcessId)
			REFERENCES	ETL.ProcessMaster (ProcessId)
			ON DELETE CASCADE
			ON UPDATE CASCADE
	);
GO

CREATE TABLE ETL.RunKeys
	(
			RunKey						INT IDENTITY(1,1)	NOT NULL
		,	StartDate					DATETIME			NOT NULL
		,	EndDate						DATETIME			NULL
		,	CONSTRAINT PK_RunKey		PRIMARY KEY NONCLUSTERED (RunKey)
	);

CREATE TABLE ETL.ProcessGroupRunLog
	(
			ProcessGroupId				INT			NOT NULL
		,	ProcessGroupStatus			VARCHAR(30)	NOT NULL
		,	ProcessGroupStartDateTime	DATETIME	NOT NULL
		,	ProcessGroupStatusDate		DATETIME	NOT NULL
		,	ProcessGroupRunKey			INT			NOT NULL
		,	CONSTRAINT	FK_ProcessGroupId2	FOREIGN KEY (ProcessGroupId)
			REFERENCES	ETL.ProcessGroupings (ProcessGroupId)
			ON DELETE NO ACTION
			ON UPDATE NO ACTION
		,	CONSTRAINT	FK_ProcessGroupRunKey	FOREIGN KEY (ProcessGroupRunKey)
			REFERENCES	ETL.RunKeys (RunKey)
			ON DELETE NO ACTION
			ON UPDATE NO ACTION
	);
GO

CREATE TABLE ETL.ProcessRunLog
	(
			ProcessId				INT			NOT NULL
		,	ProcessStatus			VARCHAR(30)	NOT NULL
		,	ProcessStartDateTime	DATETIME	NULL
		,	ProcessStatusDate		DATETIME	NOT NULL
		,	ProcessRunKey			INT			NOT NULL
		,	RunCount				INT			NULL
		,	CONSTRAINT	FK_ProcessId3	FOREIGN KEY (ProcessId)
			REFERENCES	ETL.ProcessMaster (ProcessId)
			ON DELETE NO ACTION
			ON UPDATE NO ACTION
		,	CONSTRAINT	FK_ProcessRunKey	FOREIGN KEY (ProcessRunKey)
			REFERENCES	ETL.RunKeys (RunKey)
			ON DELETE NO ACTION
			ON UPDATE NO ACTION
	);
GO

CREATE TABLE ETL.TestLog
	(
		[ETLMessage] [varchar](max) NULL,
		[ExecutionTime] [datetime] NULL
	);
GO
-- Create SPROCS

-- Create required Stored procedures
-- EXEC [ETL].[ProcessMasterSetForRun]

CREATE PROCEDURE [ETL].[ProcessMasterSetForRun]

AS

DECLARE @ProcessGroupsToRun INT
DECLARE	@RunKey INT


--	Are there processes to run now
SELECT	@ProcessGroupsToRun = COUNT(1)
--
FROM	
	(
		SELECT DISTINCT PG.ProcessGroupId
		FROM		DWUser.ETL.ProcessGroupings AS PG
		LEFT JOIN	DWUser.ETL.ProcessGroupRunLog AS PGRL
		ON			PG.ProcessGroupId = PGRL.ProcessGroupID
		LEFT JOIN	DWUser.ETL.RunKeys AS RK
		ON			PGRL.ProcessGroupRunKey = RK.RunKey
		WHERE	
				(
					PGRL.ProcessGroupId IS NULL
				OR	
					GETDATE() >= DATEADD(MINUTE, PG.ProcessGroupFrequency,RK.EndDate)
				)
		AND NOT EXISTS
				(
					SELECT	1
					FROM		DWUser.ETL.ProcessRunLog AS PRL
					LEFT JOIN	DWUser.ETL.ProcessGroupProcessBridge AS PB
					ON			PRL.ProcessID = PB.ProcessID
					LEFT JOIN	DWUser.ETL.RunKeys AS RK
					ON			RK.RunKey = PRL.ProcessRunKey
					WHERE		RK.EndDate IS NULL 
					AND			PRL.ProcessID IS NOT NULL
					AND			PB.ProcessGroupId = PG.ProcessGroupId
				)
	)	AS A
--	PRINT @ProcessGroupsToRun
--	If there are records set the processes to waiting and prepare for execution
IF @ProcessGroupsToRun > 0

	BEGIN

		--	Create a new RunKey
		INSERT INTO ETL.RunKeys (StartDate)
		SELECT GETDATE()

		SELECT @RunKey = MAX(RunKey) FROM  ETL.RunKeys

		--	Associate each Process to be run now with the new RunKey
		INSERT INTO DWUser.ETL.ProcessGroupRunLog (ProcessGroupId, ProcessGroupStatus, ProcessGroupStartDateTime, ProcessGroupStatusDate, ProcessGroupRunKey)
		SELECT		ProcessGroupId
				,	'Waiting' AS ProcessGroupStatus
				,	GETDATE() AS ProcessGroupStartDateTime
				,	GETDATE() AS ProcessGroupStatusDate
				,	@RunKey AS ProcessGroupRunKey
		FROM	
			(
				SELECT DISTINCT PG.ProcessGroupId
				FROM		DWUser.ETL.ProcessGroupings AS PG
				LEFT JOIN	DWUser.ETL.ProcessGroupRunLog AS PGRL
				ON			PG.ProcessGroupId = PGRL.ProcessGroupID
				LEFT JOIN	DWUser.ETL.RunKeys AS RK
				ON			PGRL.ProcessGroupRunKey = RK.RunKey
				WHERE	
						(
							PGRL.ProcessGroupId IS NULL
						OR	
							GETDATE() >= DATEADD(MINUTE, PG.ProcessGroupFrequency,RK.EndDate)
						)
				AND NOT EXISTS
						(
							SELECT	1
							FROM		DWUser.ETL.ProcessRunLog AS PRL
							LEFT JOIN	DWUser.ETL.ProcessGroupProcessBridge AS PB
							ON			PRL.ProcessID = PB.ProcessID
							LEFT JOIN	DWUser.ETL.RunKeys AS RK
							ON			RK.RunKey = PRL.ProcessRunKey
							WHERE		RK.EndDate IS NULL 
							AND			PRL.ProcessID IS NOT NULL
							AND			PB.ProcessGroupId = PG.ProcessGroupId
						)
			)	AS A
	--	Update All Process associated with the active ProcessGroups, set them to waiting and associate them with the current run key
	
		INSERT INTO DWUser.ETL.ProcessRunLog (ProcessId, ProcessStatus, ProcessStatusDate, ProcessRunKey, RunCount)
		SELECT	A.ProcessId  AS ProcessId
			,	'Waiting' AS ProcessStatus
			,	GETDATE() AS ProcessStatusDate
			,	@RunKey AS ProcessRunKey
			,	0 AS RunCount
		FROM
			(
				SELECT	DISTINCT PM.ProcessId 
				FROM	DWUser.ETL.ProcessMaster AS PM
				JOIN	DWUser.ETL.ProcessGroupProcessBridge AS PB
				ON		PM.ProcessID = PB.ProcessId
				JOIN	DWUser.ETL.ProcessGroupRunLog	AS PGRL
				ON		PB.ProcessGroupId = PGRL.ProcessGroupId
				WHERE	PGRL.ProcessGroupStatus = 'Waiting'
			) AS A
	END
GO

USE [DWUser]
GO

--EXEC ETL.ProcessMasterIsWorkWaiting 
CREATE PROCEDURE ETL.ProcessMasterIsWorkWaiting 

AS


SELECT	COUNT(1) AS RecordsWaiting
--SELECT *
FROM		DWUser.ETL.ProcessRunLog			AS PRL	WITH(READPAST)
LEFT JOIN	DWUser.ETL.ProcessDependencyBridge	AS DB	
ON			PRL.ProcessID = DB.ProcessID
JOIN		DWUser.ETL.ProcessRunLog			AS PRL2
ON			PRL2.ProcessID = DB.DependeeProcessID
WHERE		PRL.ProcessStatus = 'Waiting'
AND			PRL2.ProcessStatus <> 'Failed'
AND			PRL2.RunCount <= 3
AND			PRL.RunCount <= 3
GO

--EXEC ETL.ProcessRunLogWorkUpdate
CREATE PROCEDURE [ETL].[ProcessRunLogWorkUpdate] (@WorkEngineId INT)

AS

	UPDATE	PRL WITH(ROWLOCK, READPAST)
	SET		ProcessStatus = CONCAT('In Process Work Engine ',@WorkEngineId)
		,	ProcessStartDateTime	=	GETDATE()
		,	RunCount	=	RunCount +1
	--SELECT *
	FROM	DWUser.ETL.ProcessRunLog			AS PRL	
	JOIN
		(
			SELECT	 PRL.ProcessID, PRL.ProcessRunKey
			FROM	DWUser.ETL.ProcessRunLog			AS PRL		WITH(READPAST)
			LEFT JOIN	DWUser.ETL.ProcessDependencyBridge AS PB
			ON		PRL.ProcessID = PB.ProcessID
			WHERE	PRL.ProcessStatus = 'Waiting'
			AND		NOT EXISTS	(
									SELECT	1
									FROM	DWUser.ETL.ProcessRunLog	AS PRL2 WITH(READPAST)
									WHERE	
										(
											(
												PRL2.ProcessStatus = 'Waiting'
											AND
												PRL2.RunCount < 3
											)
										OR	
											PRL2.ProcessStatus LIKE 'In Process%'
										)
									AND		PB.DependeeProcessID = PRL2.ProcessID
									AND		PRL2.ProcessRunKey = PRL.ProcessRunKey
								)
		)	AS UpdateRecord
	ON	PRL.ProcessID = UpdateRecord.ProcessID
	AND PRL.ProcessRunKey = UpdateRecord.ProcessRunKey;


	DECLARE @RowCounts INT
	SET @RowCounts = (SELECT	COUNT(1)
	FROM	DWUser.ETL.ProcessRunLog			AS PRL	WITH(NOLOCK)
	WHERE	PRL.ProcessStatus = CONCAT('In Process Work Engine ',@WorkEngineId))

	IF  @RowCounts = 0
		BEGIN
			 SELECT '' AS Processname,CAST(''AS CHAR) AS ProcessExecutionString,'' AS ProcessObjectType
		END
	ELSE
		BEGIN 
			SELECT	PM.ProcessName, CAST(ISNULL(PM.ProcessExecutionString,'') AS CHAR)AS ProcessExecutionString, PM.ProcessObjectType
			FROM	DWUser.ETL.ProcessRunLog			AS PRL	WITH(NOLOCK)
			JOIN	DWUser.ETL.ProcessMaster			AS PM	WITH(NOLOCK)
			ON		PM.ProcessId	=	PRL.ProcessId
			WHERE	PRL.ProcessStatus = CONCAT('In Process Work Engine ',@WorkEngineId)
		END
GO
--EXEC ETL.ProcessMasterAreDependenciesFailed
CREATE PROCEDURE [ETL].[ProcessMasterAreDependenciesFailed] (@WorkEngineId INT, @TaskName VARCHAR(50), @ProcessName VARCHAR(50))

AS
	DECLARE		@FailedCount	INT

	SELECT		COUNT(1) AS DependenciesFailed
	FROM		DWUser.ETL.ProcessRunLog			AS PRL	
	LEFT JOIN	DWUser.ETL.ProcessDependencyBridge	AS DB	
	ON			PRL.ProcessID = DB.ProcessID
	JOIN		DWUser.ETL.ProcessRunLog			AS PRL2
	ON			PRL2.ProcessID = DB.DependeeProcessID
	WHERE		PRL.ProcessStatus =  CONCAT('In Process Work Engine ',@WorkEngineId)
	AND	
		(
			PRL2.ProcessStatus = 'Failed'
		OR
			PRL2.RunCount = 3
		);

IF	@FailedCount > 0
	BEGIN
		INSERT INTO DWUser.ETL.TestLog (ETLMessage,ExecutionTime)
		SELECT CONCAT('Process ',@ProcessName,' Cannot execute becauese one or more processes it is dependent on have failed. This failure was reported by task ',@TaskName,' for Work Engine ',CAST(@WorkEngineId AS CHAR)),GETDATE();
	END;
GO

CREATE PROCEDURE ETL.ProcessMasterUpdateFailure	(@WorkEngineId INT, @TaskName VARCHAR(50), @ProcessName VARCHAR(50))

AS

UPDATE	AP WITH(ROWLOCK)
SET		ProcessStatus = 'Failed'
--SELECT *
FROM	DWUser.ETL.ProcessRunLog			AS AP	
WHERE	ProcessStatus = CONCAT('In Process Work Engine ',@WorkEngineId)

INSERT INTO DWUser.ETL.TestLog (ETLMessage,ExecutionTime)
SELECT CONCAT('Process ',@ProcessName,' failed. This failure was reported by task ',@TaskName,' for Work Engine ',CAST(@WorkEngineId AS CHAR)),GETDATE();
GO


CREATE PROCEDURE [ETL].[ProcessMasterUpdateCompletion]	(@WorkEngineId INT, @TaskName VARCHAR(50), @ProcessName VARCHAR(50))

AS

UPDATE	PRL WITH(ROWLOCK, READPAST)
SET		ProcessStatus = 'Completion'
,		ProcessStatusDate = GETDATE()
--SELECT *
FROM	DWUser.ETL.ProcessRunLog			AS PRL	
WHERE	ProcessStatus = CONCAT('In Process Work Engine ',CAST(@WorkEngineId AS CHAR))
AND		RunCount < 3;


INSERT INTO DWUser.ETL.TestLog (ETLMessage,ExecutionTime)
SELECT CONCAT('Process ',@ProcessName,' success. This success was reported by task ',@TaskName,' for Work Engine ',CAST(@WorkEngineId AS CHAR)),GETDATE();

DECLARE @RunKeysToClose INT;
SELECT @RunKeysToClose = COUNT(1)
FROM	ETL.RunKeys	AS RK
WHERE	EndDate IS NULL
AND NOT EXISTS
(
	SELECT		1
	FROM		DWUser.ETL.ProcessRunLog AS PRL
	WHERE		PRL.ProcessStatus <> 'Completion'
	AND			PRL.ProcessRunKey = RK.RunKey
);

IF @RunKeysToClose > 0
	BEGIN
		UPDATE	RK
		SET		EndDate = GETDATE()
		--SELECT *
		FROM	ETL.RunKeys	AS RK
		WHERE	EndDate IS NULL
		AND NOT EXISTS
		(
			SELECT		1
			FROM		DWUser.ETL.ProcessRunLog AS PRL
			WHERE		PRL.ProcessStatus <> 'Completion'
			AND			PRL.ProcessRunKey = RK.RunKey
		)


		INSERT INTO DWUser.ETL.TestLog (ETLMessage,ExecutionTime)
		SELECT CONCAT('Closed ',CAST(@RunKeysToClose AS CHAR),' RunKeys'),GETDATE();
	END;
	

DECLARE @ProcessGroupsToClose INT;
SELECT @ProcessGroupsToClose = COUNT(1)
FROM	DWUser.ETL.ProcessGroupRunLog	AS PGRL
JOIN	DWUser.ETL.ProcessGroupProcessBridge AS PB
ON		PB.ProcessGroupId = PGRL.ProcessGroupId
WHERE	NOT EXISTS
(
	SELECT		1
	FROM		DWUser.ETL.ProcessRunLog AS PRL
	WHERE		PRL.ProcessStatus <> 'Completion'
	AND			PRL.ProcessId = PB.ProcessId
);

IF @ProcessGroupsToClose > 0
	BEGIN
		UPDATE	PGRL
		SET		ProcessGroupStatus = 'Completion'
			,	ProcessGroupStatusDate = GETDATE()
		--SELECT *
		FROM	DWUser.ETL.ProcessGroupRunLog	AS PGRL
		WHERE	ProcessGroupStatus <> 'Completion'
		AND NOT EXISTS
		(
			SELECT		1
			FROM		DWUser.ETL.ProcessRunLog AS PRL
			JOIN		DWUser.ETL.ProcessGroupProcessBridge AS PB
			ON			PB.ProcessId = PRL.ProcessId
			WHERE		PRL.ProcessStatus <> 'Completion'
			AND			PB.ProcessGroupId = PGRL.ProcessGroupId
		)


		INSERT INTO DWUser.ETL.TestLog (ETLMessage,ExecutionTime)
		SELECT CONCAT('Closed ',CAST(@ProcessGroupsToClose AS CHAR),' RunKeys'),GETDATE();
	END;

GO

CREATE PROCEDURE [ETL].[ProcessMasterDelay]

AS
DECLARE @DELAY DATETIME
SET @DELAY = (SELECT CAST(CONCAT('00:',RIGHT(RTRIM('0' + CAST(ABS(CHECKSUM(NEWID())) % 2  AS CHAR)),2),':',RIGHT(RTRIM('0' + CAST(ABS(CHECKSUM(NEWID())) % 59 + 1 AS CHAR)),2))AS TIME))
--SET @DELAY = '00:00:01'
BEGIN  
    WAITFOR DELAY @DELAY;  
END;  
GO

-- Populate Tables

INSERT INTO ETL.ProcessMaster
	(
			ProcessName
		,	ProcessObjectType		
		,	ProcessExecutionString	
		,	ProcessPriority			
		,	IsActive					
		,	IncrementalLoadFieldSource
		,	IncrementalLoadField			
		,	CreatedBy				
		,	CreatedDateTime			
		,	ModifiedBy				
		,	ModifiedDateTime
	)
VALUES
('RawData1','SSIS','Test.dtsx',1,1,'dbo.Data1','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData2','SSIS','Test.dtsx',1,1,'dbo.Data2','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData3','SSIS','Test.dtsx',1,1,'dbo.Data3','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData4','SSIS','Test.dtsx',1,1,'dbo.Data4','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData5','SSIS','Test.dtsx',1,1,'dbo.Data5','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData6','SSIS','Test.dtsx',1,1,'dbo.Data6','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData7','SSIS','Test.dtsx',1,1,'dbo.Data7','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData8','SSIS','Test.dtsx',1,1,'dbo.Data8','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData9','SSIS','Test.dtsx',1,1,'dbo.Data9','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData10','SSIS','Test.dtsx',1,1,'dbo.Data10','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData11','SSIS','Test.dtsx',1,1,'dbo.Data11','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData12','SSIS','Test.dtsx',1,1,'dbo.Data12','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData13','SSIS','Test.dtsx',1,1,'dbo.Data13','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData14','SSIS','Test.dtsx',1,1,'dbo.Data14','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData15','SSIS','Test.dtsx',1,1,'dbo.Data15','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData16','SSIS','Test.dtsx',1,1,'dbo.Data16','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData17','SSIS','Test.dtsx',1,1,'dbo.Data17','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData18','SSIS','Test.dtsx',1,1,'dbo.Data18','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData19','SSIS','Test.dtsx',1,1,'dbo.Data19','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('RawData20','SSIS','Test.dtsx',1,1,'dbo.Data20','ModifiedDateTime','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim1','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim2','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim3','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim4','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim5','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim6','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim7','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim8','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim9','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim10','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim11','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim12','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim13','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim14','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim15','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim16','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim17','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim18','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim19','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DataDim20','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DerivedDim1','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DerivedDim2','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DerivedDim3','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DerivedDim4','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('DerivedDim5','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('Fact1','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('Fact2','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('Fact3','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('Fact4','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL),
('Fact5','SQL','ETL.ProcessMasterDelay',1,1,'NULL','NULL','Gerald.Spangler',GETDATE(),NULL,NULL);
GO

INSERT INTO	ETL.ProcessDependencyBridge
	(
			ProcessId			
		,	DependeeProcessID	
		,	CreatedBy			
		,	CreatedDateTime		
		,	ModifiedBy			
		,	ModifiedDateTime	
	)
VALUES(50,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(50,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(50,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(50,24,'Gerald.Spangler',GETDATE(),NULL,NULL),
(50,25,'Gerald.Spangler',GETDATE(),NULL,NULL),
(21,1,'Gerald.Spangler',GETDATE(),NULL,NULL),
(22,2,'Gerald.Spangler',GETDATE(),NULL,NULL),
(23,3,'Gerald.Spangler',GETDATE(),NULL,NULL),
(24,4,'Gerald.Spangler',GETDATE(),NULL,NULL),
(25,5,'Gerald.Spangler',GETDATE(),NULL,NULL),
(49,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(49,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(49,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(49,29,'Gerald.Spangler',GETDATE(),NULL,NULL),
(49,30,'Gerald.Spangler',GETDATE(),NULL,NULL),
(26,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
(27,7,'Gerald.Spangler',GETDATE(),NULL,NULL),
(28,8,'Gerald.Spangler',GETDATE(),NULL,NULL),
(29,9,'Gerald.Spangler',GETDATE(),NULL,NULL),
(30,10,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,31,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,32,'Gerald.Spangler',GETDATE(),NULL,NULL),
(48,33,'Gerald.Spangler',GETDATE(),NULL,NULL),
(31,11,'Gerald.Spangler',GETDATE(),NULL,NULL),
(32,12,'Gerald.Spangler',GETDATE(),NULL,NULL),
(33,13,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,34,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,35,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,36,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(47,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(34,14,'Gerald.Spangler',GETDATE(),NULL,NULL),
(35,15,'Gerald.Spangler',GETDATE(),NULL,NULL),
(36,16,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,37,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,38,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,39,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,40,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,41,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,42,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,43,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,44,'Gerald.Spangler',GETDATE(),NULL,NULL),
(46,45,'Gerald.Spangler',GETDATE(),NULL,NULL),
(37,17,'Gerald.Spangler',GETDATE(),NULL,NULL),
(38,18,'Gerald.Spangler',GETDATE(),NULL,NULL),
(39,19,'Gerald.Spangler',GETDATE(),NULL,NULL),
(40,20,'Gerald.Spangler',GETDATE(),NULL,NULL),
(41,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(41,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(41,25,'Gerald.Spangler',GETDATE(),NULL,NULL),
(42,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(42,29,'Gerald.Spangler',GETDATE(),NULL,NULL),
(42,31,'Gerald.Spangler',GETDATE(),NULL,NULL),
(43,33,'Gerald.Spangler',GETDATE(),NULL,NULL),
(43,35,'Gerald.Spangler',GETDATE(),NULL,NULL),
(43,37,'Gerald.Spangler',GETDATE(),NULL,NULL),
(44,39,'Gerald.Spangler',GETDATE(),NULL,NULL),
(44,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(44,24,'Gerald.Spangler',GETDATE(),NULL,NULL),
(45,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(45,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(45,30,'Gerald.Spangler',GETDATE(),NULL,NULL);
GO

INSERT INTO	ETL.ProcessGroupings
	(
			ProcessGroupName		
		,	ProcessGroupFrequency	
		,	MaxConcurrent			
		,	CreatedBy				
		,	CreatedDateTime			
		,	ModifiedBy				
		,	ModifiedDateTime		
	)
VALUES
('SimpleProcess1',5,5,'Gerald.Spangler',GETDATE(),NULL,NULL),
('SimpleProcess2',10,5,'Gerald.Spangler',GETDATE(),NULL,NULL),
('MediumProcess1',60,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
('MediumProcess2',240,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
('LargeProcess1',1440,17,'Gerald.Spangler',GETDATE(),NULL,NULL);
GO

INSERT INTO ETL.ProcessGroupProcessBridge
	(
			ProcessGroupId	
		,	ProcessId		
		,	CreatedBy		
		,	CreatedDateTime	
		,	ModifiedBy		
		,	ModifiedDateTime
	)
VALUES
(1,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,24,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,25,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,1,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,2,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,3,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,4,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,5,'Gerald.Spangler',GETDATE(),NULL,NULL),
(1,50,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,29,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,30,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,7,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,8,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,9,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,10,'Gerald.Spangler',GETDATE(),NULL,NULL),
(2,49,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,31,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,32,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,33,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,11,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,12,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,13,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,1,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,2,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,3,'Gerald.Spangler',GETDATE(),NULL,NULL),
(3,48,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,34,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,35,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,36,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,14,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,15,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,16,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,7,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,8,'Gerald.Spangler',GETDATE(),NULL,NULL),
(4,47,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,1,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,2,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,3,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,4,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,5,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,6,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,7,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,8,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,9,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,10,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,11,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,13,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,15,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,17,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,18,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,19,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,20,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,21,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,22,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,23,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,24,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,25,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,26,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,27,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,28,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,29,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,30,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,31,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,33,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,35,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,37,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,38,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,39,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,40,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,41,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,42,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,43,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,44,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,45,'Gerald.Spangler',GETDATE(),NULL,NULL),
(5,46,'Gerald.Spangler',GETDATE(),NULL,NULL);


SELECT * FROM ETL.ProcessMaster;
SELECT * FROM ETL.ProcessDependencyBridge;
SELECT * FROM ETL.ProcessGroupings;
SELECT * FROM ETL.ProcessGroupProcessBridge;
SELECT * FROM ETL.RunKeys;
SELECT * FROM ETL.ProcessGroupRunLog;
SELECT * FROM ETL.ProcessRunLog;
SELECT * FROM ETL.TestLog;
GO


/*	SQL Agent Job Script, Numbr of jobs to execute needs moved from a query to a MetaDataTable


USE DWUser

EXEC DWUser.ETL.ProcessMasterSetForRun

	PRINT 'ProcessMasterSetForRun Completed'

DECLARE	@Engines		INT
DECLARE @execution_id	BIGINT
DECLARE @var0			SMALLINT = 1

SET	@Engines	=	CASE WHEN @@SERVERNAME = 'P10QADWDB01' THEN 7 WHEN @@SERVERNAME = 'P10-DWBD-03' THEN 15 WHEN @@SERVERNAME = 'P10-DWDB-01' THEN 9 ELSE 8 END

	PRINT	CONCAT('The Execution Server is ',@@SERVERNAME)
	PRINT	CONCAT('There will be ',@Engines,' work engines executed')

WHILE @Engines > 0

	BEGIN
		EXEC [SSISDB].[catalog].[create_execution] @package_name=N'WorkEngine.dtsx',
			@execution_id=@execution_id OUTPUT,
			@folder_name=N'CM',
			  @project_name=N'ETL_Prototype_V1',
  			@use32bitruntime=False,
			  @reference_id=Null
		Select @execution_id
		PRINT CONCAT('Excution Id = ',@execution_id)

		EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,
			@object_type=50,
			  @parameter_name=N'LOGGING_LEVEL',
			  @parameter_value=@var0
		EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id,
			@object_type=30,
			 @parameter_name=N'WorkEngineId',
			  @parameter_value=@Engines
		EXEC [SSISDB].[catalog].[start_execution] @execution_id

			PRINT	CONCAT('Work Engine ',@Engines,' has been executed')
		SET @Engines = @Engines -1
			PRINT	CONCAT('There will be ',@Engines,' additional work engines executed')
	END


--*/


/*	Testing Code

SELECT * FROM ETL.RunKeys;
SELECT * FROM ETL.ProcessGroupRunLog ORDER BY ProcessGroupId, ProcessGroupRunKey;
SELECT * FROM ETL.ProcessRunLog ORDER BY ProcessId, ProcessRunKey;

EXEC DWUser.ETL.ProcessMasterSetForRun

SELECT * FROM ETL.RunKeys;
SELECT * FROM ETL.ProcessGroupRunLog ORDER BY ProcessGroupId, ProcessGroupRunKey;
SELECT * FROM ETL.ProcessDependencyBridge;
SELECT * FROM ETL.ProcessRunLog WITH(NOLOCK)  ORDER BY ProcessId, ProcessRunKey;


SELECT	PRL1.ProcessId, PRL1.ProcessStatusDate, PM1.ProcessName, PRL2.ProcessId, PRL2.ProcessStatusDate, PM2.ProcessName 
FROM	DWuser.ETL.ProcessRunLog AS PRL1
JOIN	DWUser.ETL.ProcessMaster AS PM1
ON		PRL1.ProcessId = PM1.ProcessId
JOIN	DWUser.ETL.ProcessDependencyBridge AS PDB
ON		PDB.ProcessId = PM1.ProcessID
JOIN	DWuser.ETL.ProcessRunLog AS PRL2
ON		PDB.DependeeProcessId = PRL2.ProcessId
JOIN	DWUser.ETL.ProcessMaster AS PM2
ON		PRL2.ProcessId = PM2.ProcessId
WHERE	PRL1.ProcessRunKey = 4
AND		PRL2.ProcessRunKey = 4
AND		PRL1.ProcessStatusDate >= PRL2.ProcessStatusDate
--*/


/*		Dev Notes

	*	Confirm Dependency is being honored during execution		--Confirmed
	*	Clean up logging, info isn't useful enough
		* Include ProcessId, StartTime, EndTime, Runkey, ProcessStatus
		* Is ProcessGroup Id Relevent?
	*	Count of key closures is wrong
	*	Seperate keys to the Process level?

--*/