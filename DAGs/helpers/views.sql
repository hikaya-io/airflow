USE newdea_db
GO

CREATE VIEW [dbo].[OGP]
AS
    SELECT regionGrp.name country_name, fundingSourceGrp.name funding_source, Project.orgunitId project_id, Project.name project_name,
        Project.startDate project_start, Project.endDate project_end, Project.StatusId project_status
    FROM OrgUnit mainOrg
        LEFT JOIN OrgUnit topLevelUnit ON mainOrg.id = topLevelUnit.parentId
        LEFT JOIN [Group] topLevelGrp ON topLevelUnit.id = topLevelGrp.orgUnitId
        LEFT JOIN OrgUnit regionUnit ON topLevelUnit.id = regionUnit.parentId
        LEFT JOIN [Group] regionGrp ON regionUnit.id = regionGrp.orgUnitId
        LEFT JOIN OrgUnit fundingSourceUnit ON regionUnit.id = fundingSourceUnit.parentId
        LEFT JOIN [Group] fundingSourceGrp ON fundingSourceUnit.id = fundingSourceGrp.orgUnitId
        LEFT JOIN OrgUnit projectUnit ON fundingSourceUnit.id = projectUnit.parentId
        LEFT JOIN Project ON projectUnit.id = Project.orgunitId
    WHERE mainOrg.parentId IS NULL AND topLevelGrp.orgunitId = 37637 AND Project.orgunitId IS NOT NULL
GO

EOM