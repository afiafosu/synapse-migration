CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_hr_employee_current_assignment_pay`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    -- 2) Si existe staging, bórrala
    DROP TABLE IF EXISTS main.gold.HR_EmployeeCurrentAssignmentPay_stg;
    
    -- 3) CTAS: construye tabla gold staging desde SOLO tus 4 tablas silver externas
    CREATE OR REPLACE TABLE main.gold.HR_EmployeeCurrentAssignmentPay_stg AS
    WITH Dept AS (
        SELECT
            CAST(DepartmentID AS int)          AS DepartmentID,
            CAST(`Name` AS STRING)      AS DepartmentName,
            CAST(GroupName AS STRING)   AS DepartmentGroupName
        FROM main.silver.humanresources_Department),
    Shft AS (
        SELECT
            CAST(ShiftID AS int)               AS ShiftID,
            CAST(`Name` AS STRING)      AS ShiftName,
            CAST(StartTime AS STRING)     AS ShiftStartTimeText,  -- evita problemas con TIME
            CAST(EndTime AS STRING)       AS ShiftEndTimeText
        FROM main.silver.humanresources_Shift),
    DeptHistRaw AS (
        SELECT
            CAST(BusinessEntityID AS int)      AS BusinessEntityID,
            CAST(DepartmentID AS int)          AS DepartmentID,
            CAST(ShiftID AS int)               AS ShiftID,
            CAST(StartDate AS date)            AS StartDate,
            CAST(EndDate AS date)              AS EndDate
        FROM main.silver.humanresources_EmployeeDepartmentHistory    ),
    DeptHistRanked AS(
        SELECT
            r.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY r.BusinessEntityID
                ORDER BY
                    CASE WHEN r.EndDate IS NULL THEN 0 ELSE 1 END,
                    r.StartDate DESC
            ) AS rn
        FROM DeptHistRaw r),
    CurrentDept AS (
        SELECT
            BusinessEntityID,
            DepartmentID,
            ShiftID,
            StartDate AS DeptStartDate,
            EndDate   AS DeptEndDate
        FROM DeptHistRanked
        WHERE rn = 1),
    PayHistRaw AS (
        SELECT
            CAST(BusinessEntityID AS int)        AS BusinessEntityID,
            CAST(Rate AS decimal(19,4))          AS PayRate,
            CAST(PayFrequency AS tinyint)        AS PayFrequency,
            CAST(RateChangeDate AS date)         AS RateChangeDate
        FROM main.silver.humanresources_EmployeePayHistory),
    PayHistRanked AS (
        SELECT
            p.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY p.BusinessEntityID
                ORDER BY p.RateChangeDate DESC, p.PayRate DESC
            ) AS rn
        FROM PayHistRaw p),
    CurrentPay AS (
        SELECT
            BusinessEntityID,
            PayRate,
            PayFrequency,
            RateChangeDate
        FROM PayHistRanked
        WHERE rn = 1),
    EmployeeUniverse AS (
        SELECT BusinessEntityID FROM DeptHistRaw
        UNION
        SELECT BusinessEntityID FROM PayHistRaw)
    SELECT
        u.BusinessEntityID,

        cd.DepartmentID,
        d.DepartmentName,
        d.DepartmentGroupName,

        cd.ShiftID,
        s.ShiftName,
        s.ShiftStartTimeText,   -- texto para evitar TIME issues
        s.ShiftEndTimeText,

        cd.DeptStartDate,
        cd.DeptEndDate,

        cp.PayRate,
        cp.PayFrequency,
        cp.RateChangeDate,

        UUID()           AS LoadRunId,
        current_timestamp()  AS LoadDate
    FROM EmployeeUniverse u
    LEFT JOIN CurrentDept cd  ON cd.BusinessEntityID  = u.BusinessEntityID
    LEFT JOIN Dept d          ON d.DepartmentID       = cd.DepartmentID
    LEFT JOIN Shft s          ON s.ShiftID            = cd.ShiftID
    LEFT JOIN CurrentPay cp   ON cp.BusinessEntityID  = u.BusinessEntityID;
    
    -- 4) "Swap" de tablas: renombra target actual a _old, y staging a definitiva
    IF (SELECT 1 FROM system.information_schema.tables WHERE table_catalog = 'main' AND table_schema = 'gold' AND table_name = 'hr_employeecurrentassignmentpay') == 1 THEN
        DROP TABLE IF EXISTS main.gold.HR_EmployeeCurrentAssignmentPay_old;
        
        ALTER TABLE main.gold.HR_EmployeeCurrentAssignmentPay
        RENAME TO main.gold.HR_EmployeeCurrentAssignmentPay_old;
        
        ALTER TABLE main.gold.HR_EmployeeCurrentAssignmentPay_stg
        RENAME TO main.gold.HR_EmployeeCurrentAssignmentPay;
        
        DROP TABLE IF EXISTS main.gold.HR_EmployeeCurrentAssignmentPay_old;
    ELSE
        ALTER TABLE main.gold.HR_EmployeeCurrentAssignmentPay_stg
        RENAME TO main.gold.HR_EmployeeCurrentAssignmentPay;
    END IF;
END;
