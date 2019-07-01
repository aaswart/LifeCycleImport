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

	SELECT @userID = u.Id FROM tblUser as u WHERE u.Name = @UserName
	
	PRINT 'Import ' + @ActionName + ' of ' + @UserName + ' to custmer: ' + @CompanyName + ', project: ' + @ProjectName + ', task: ' + @TaskName

	SELECT @taskId = t.ID FROM tblTask as t
		WHERE t.ProjectID = (SELECT p.ID FROM tblProject as p
							WHERE p.CustomerID = (SELECT c.ID FROM tblCustomer as c WHERE c.CompanyName = @CompanyName)
								AND p.Name = @ProjectName)
			AND t.Name = @TaskName

	--PRINT ISNULL(CAST(@taskId as varchar(20)),'No task id')

	SELECT @lastDate = CAST(MAX(h.WorkDate) as DATE)
		FROM tblHours as h WHERE h.TaskID = @taskId AND h.UserID = @userId

	--PRINT CONVERT(varchar(20),@lastDate,120)

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
		WHERE v.ActionName LIKE @ActionName + '%'
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
SET @toDate = '2019-07-01'

--##########################################
--Get AMS
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


GO
/*
	SELECT t.ID FROM tblTask as t
	WHERE t.ProjectID = (SELECT p.ID FROM tblProject as p
							WHERE p.CustomerID = (SELECT c.ID FROM tblCustomer as c WHERE c.CompanyName = 'Elburg Yachting')
								AND p.Name = 'Algemeen')
		AND t.Name = 'Support'
GO

SELECT DISTINCT [ NAME] FROM LC_export
*/
--COMMIT
ROLLBACK