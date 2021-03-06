BEGIN TRANSACTION
GO
DROP VIEW [dbo].[vwLC_export]
DROP PROCEDURE  [dbo].[spImportLC_exportData] 	
GO
CREATE VIEW [dbo].[vwLC_export]
AS
	SELECT 
		CAST(x.StartLocal as DATE) as LocalDate,	
		LTRIM(x.ActionName) as ActionName,
		MAX(x.ActionNote) as ActionNote,
		SUM(CAST(DATEDIFF(MINUTE,x.StartUtc,x.EndUtc) as decimal(10,2)) / 60) as DurationHour
	FROM 
	(
	SELECT 
		CONVERT(datetime,[START DATE(UTC)], 120) as StartUtc,
		CONVERT(datetime,[END DATE(UTC)], 120) as EndUtc,
		CONVERT(datetime,SUBSTRING([START TIME(LOCAL)],0,18) , 120) as StartLocal,
		CONVERT(datetime,SUBSTRING([END TIME(LOCAL)],0,18) , 120) as EndLocal,
		[DURATION],
		LTRIM([NAME]) as ActionName,
		LTRIM([NOTE]) as ActionNote
	FROM LC_export
	) as x
	GROUP BY
		CAST(x.StartLocal as DATE),
		x.ActionName

GO
CREATE  PROCEDURE [dbo].[spImportLC_exportData] 	
	@toDate datetime,
	@UserName varchar(50), 
	@CompanyName varchar(50), 
	@ProjectName varchar(50),
	@TaskName varchar(50),
	@ActionName varchar(50)
AS
BEGIN

	DECLARE @userId int
	DECLARE @taskId int
	DECLARE @lastDate datetime
	DECLARE @message nvarchar(max)
	DECLARE @errorMessage nvarchar(max)

	SELECT @userID = u.Id FROM tblUser as u WHERE u.Name = @UserName
	IF @userID IS NULL
	BEGIN
		SET @errorMessage = 'User '+@UserName+' not found'
		RAISERROR (@errorMessage, 16, 1);  
	END

	SET @message = @ActionName + ' of ' + @UserName + ' (' + CAST(@userID as varchar(10)) + ') to custmer: ' + @CompanyName + ', project: ' + @ProjectName + ', task: ' + @TaskName
	Print '_______________________'
	Print 'Import ' + @message

	SELECT @taskId = t.ID FROM tblTask as t
		WHERE t.ProjectID = (SELECT p.ID FROM tblProject as p
							WHERE p.CustomerID = (SELECT c.ID FROM tblCustomer as c WHERE c.CompanyName = @CompanyName)
								AND p.Name = @ProjectName)
			AND t.Name = @TaskName

	PRINT 'TaskID: ' + ISNULL(CAST(@taskId as varchar(20)),'No task id')

	IF @taskId IS NULL
	BEGIN
		SET @errorMessage = 'Error task not found for: ' + @message
		RAISERROR (@errorMessage, 16, 1);  
	END

	SELECT @lastDate = ISNULL(CAST(MAX(h.WorkDate) as DATE),'2000-01-01')
		FROM tblHours as h WHERE h.TaskID = @taskId AND h.UserID = @userId

	PRINT 'Last date: ' + CONVERT(varchar(20),@lastDate,120)

	INSERT INTO [dbo].[tblHours]
           ([TaskID]
           ,[UserID]
           ,[WorkDate]
           ,[WorkedHours]
           ,[HourTypeID]
           ,[Comment])
	SELECT 
		@taskId as [TaskID],
		@userID as [UserID],
		LocalDate as [WorkDate],
		FLOOR(v.DurationHour+0.5) as [WorkedHours],
		'100%' as [HourTypeID],
		v.ActionNote as [Comment]
		FROM vwLC_export as v
		WHERE LOWER(RTRIM(LTRIM(v.ActionName))) = LOWER(RTRIM(LTRIM(@ActionName)))
			AND LocalDate > @lastDate
			AND LocalDate < @toDate

	--for testing
	/*
	SELECT v.*, FLOOR(v.DurationHour+0.5) as roundend FROM vwLC_export as v
		WHERE v.ActionName = @ActionName
			AND LocalDate > @lastDate
			AND LocalDate < @toDate
	*/
END
GO
DECLARE @toDate datetime
SET @toDate = '2020-05-01'


--##########################################
--Get AMS
EXEC spImportLC_exportData 
	@toDate = @toDate, 
	@UserName = 'auke',
	@CompanyName = 'Amsterdam Software',
	@ProjectName = 'General',
	@TaskName = 'Stripe Integration',
	@ActionName = 'Work AmsSoft Stripe'

EXEC spImportLC_exportData 
	@toDate = @toDate, 
	@UserName = 'auke',
	@CompanyName = 'Amsterdam Software',
	@ProjectName = 'General',
	@TaskName = 'Support',
	@ActionName = 'Work AmsSoft'


--##########################################
--Get VTOMN 
EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'VTON',
	@ProjectName = 'Application',
	@TaskName = '00 Support',
	@ActionName = 'Work VTON'

--##########################################
--Get Bagchain 
EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'R-Two',
	@ProjectName = 'Bagchain',
	@TaskName = 'Build',
	@ActionName = 'Work Bagchain'

--##########################################
--Get hackaton 2019 
EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'hackaton 2019',
	@ProjectName = 'DigitalMe',
	@TaskName = 'DigitalMe',
	@ActionName = 'Work Hackathon'

--##########################################
--Get Elburg Yachting
EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'Elburg Yachting',
	@ProjectName = 'Algemeen',
	@TaskName = 'Support',
	@ActionName = 'Work Elburg'

EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'C Sharp Sports',
	@ProjectName = 'scouting app',
	@TaskName = 'build',
	@ActionName = 'Work CSharpSports'

EXEC spImportLC_exportData 
	@toDate= @toDate, 
	@UserName = 'auke',
	@CompanyName = 'Geje Truck Hydraulics BV',
	@ProjectName = 'Logic4 Kardex interface',
	@TaskName = 'build',
	@ActionName = 'Work Geje'

	/*
	SELECT t.ID FROM tblTask as t
	WHERE t.ProjectID = (SELECT p.ID FROM tblProject as p
							WHERE p.CustomerID = (SELECT c.ID FROM tblCustomer as c WHERE c.CompanyName = 'C Sharp Sports')
								AND p.Name = 'scouting app')
		AND t.Name = 'build'

	SELECT * FROM tblHours WHERE TaskID = 2290
	
	SELECT DISTINCT [NAME] FROM LC_export ORDER BY [NAME]
	*/
GO



--COMMIT
ROLLBACK