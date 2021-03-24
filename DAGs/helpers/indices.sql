USE newdea_db
GO
CREATE NONCLUSTERED INDEX IX_Indicator_startMonthId_endMonthId_templateId
    ON dbo.Indicator (startMonthId, endMonthId, templateId);   
GO  
CREATE NONCLUSTERED INDEX IX_IndicatorBTDataSetValues_Valuetype
    ON dbo.IndicatorBTDataSetValues (ValueType);   
GO
CREATE NONCLUSTERED INDEX IX_IndicatorValues_valueDate
    ON dbo.IndicatorValue (valueDate);   
GO
CREATE NONCLUSTERED INDEX IX_Metric_type
    ON dbo.Metric (type);   
GO

EOM