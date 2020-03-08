SELECT	PG.ProcessGroupName
	,	PGRL.ProcessGroupID
	,	PGRL.ProcessGroupStartDateTime
	,	PGRL.ProcessGroupStatusDate
	,	CAST(PGRL.ProcessGroupStatusDate - PGRL.ProcessGroupStartDateTime AS TIME) AS RunTime
--SELECT * 
FROM	DWUser.ETL.ProcessGroupRunLog	AS PGRL
JOIN	DWUser.ETL.ProcessGroupings		AS PG
ON		PGRL.ProcessGroupId = PG.ProcessGroupId
ORDER BY PGRL.ProcessGroupId
