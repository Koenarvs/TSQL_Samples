SELECT	PM.ProcessName
	,	PRL.ProcessId
	,	PRL.ProcessStartDateTime
	,	PRL.ProcessStatusDate
	,	CAST(PRL.ProcessStatusDate - PRL.ProcessStartDateTime AS TIME)  AS RunTime
--SELECT * 
FROM	DWUser.ETL.ProcessRunLog	AS	PRL
JOIN	DWUser.ETL.ProcessMaster	AS	PM
ON		PRL.ProcessId	=	PM.ProcessId
ORDER BY PRL.ProcessId