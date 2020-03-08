SELECT	PRL1.ProcessId
	,	PRL1.ProcessStatusDate
	,	PRL1.ProcessRunKey
	,	PM1.ProcessName
	,	PRL2.ProcessId
	,	PRL2.ProcessStatusDate
	,	PRL2.ProcessRunKey
	,	PM2.ProcessName 
FROM	DWuser.ETL.ProcessRunLog AS PRL1 WITH(NOLOCK)
LEFT JOIN	DWUser.ETL.ProcessMaster AS PM1 WITH(NOLOCK)
ON		PRL1.ProcessId = PM1.ProcessId
LEFT JOIN	DWUser.ETL.ProcessDependencyBridge AS PDB WITH(NOLOCK)
ON		PDB.ProcessId = PM1.ProcessID
LEFT JOIN	DWuser.ETL.ProcessRunLog AS PRL2 WITH(NOLOCK)
ON		PDB.DependeeProcessId = PRL2.ProcessId
AND		PRL1.ProcessRunKey = PRL2.ProcessRunKey
LEFT JOIN	DWUser.ETL.ProcessMaster AS PM2 WITH(NOLOCK)
ON		PRL2.ProcessId = PM2.ProcessId
WHERE	PRL1.ProcessStatus = 'Completion'
AND		ISNULL(PRL1.ProcessStatus,'Completion') = 'Completion'
AND		PRL2.ProcessStatusDate > PRL1.ProcessStatusDate