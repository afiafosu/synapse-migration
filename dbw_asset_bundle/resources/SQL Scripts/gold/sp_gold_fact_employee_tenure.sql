CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_fact_employee_tenure`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    -- Si existe la tabla fact, la eliminamos (full refresh)
    DROP TABLE IF EXISTS main.gold.FactEmployeeTenure;
    
    /* 
       CTAS: calcula tenure usando solo tablas silver disponibles.
       Guardamos AsOfDate como fecha de cálculo (hoy UTC).
    */
    
    CREATE OR REPLACE TABLE main.gold.FactEmployeeTenure AS
    SELECT
        e.BusinessEntityID,
        -- Primera asignación conocida (proxy de "hire/start")
        e.TenureStartDate,
        -- Fecha de cálculo (hoy UTC)
        e.AsOfDate,
        -- Tenure en días (si no hay startdate, queda NULL)
        CASE 
            WHEN e.TenureStartDate IS NULL THEN NULL
            ELSE DATEDIFF(e.TenureStartDate, e.AsOfDate)
        END AS TenureDays,
        -- Tenure aproximado en años (decimal)
        CASE
            WHEN e.TenureStartDate IS NULL THEN NULL
            ELSE CAST(DATEDIFF(e.TenureStartDate, e.AsOfDate) AS decimal(19,4)) / 365.25
        END AS TenureYears,
        -- Activo según existencia de asignación vigente a AsOfDate (proxy)
        e.IsActive,
        -- Enriquecimiento: dept/shift actuales (si aplica)
        cur.DepartmentID,
        d.`Name`    AS DepartmentName,
        d.GroupName AS DepartmentGroupName,
        cur.ShiftID,
        s.`Name`    AS ShiftName,
        -- Audit
        current_timestamp() AS LoadDate
    FROM
    (
        SELECT
            x.BusinessEntityID,
            x.TenureStartDate,
            CAST(current_timestamp() AS date) AS AsOfDate,
            x.IsActive
        FROM
        (
            SELECT
                edh.BusinessEntityID,
                MIN(edh.StartDate) AS TenureStartDate,
                CASE
                    WHEN SUM(CASE 
                              WHEN edh.StartDate <= CAST(current_timestamp() AS date)
                               AND (edh.EndDate IS NULL OR edh.EndDate >= CAST(current_timestamp() AS date))
                              THEN 1 ELSE 0 END) > 0
                    THEN 1 ELSE 0
                END AS IsActive
            FROM main.silver.humanresources_EmployeeDepartmentHistory edh
            GROUP BY edh.BusinessEntityID
        ) x
    ) e
    -- Current assignment (sin ROW_NUMBER: usamos MAX(StartDate) vigente)
    LEFT JOIN
    (
        SELECT
            edh2.BusinessEntityID,
            edh2.DepartmentID,
            edh2.ShiftID,
            edh2.StartDate
        FROM main.silver.humanresources_EmployeeDepartmentHistory edh2
        JOIN
        (
            SELECT
                BusinessEntityID,
                MAX(StartDate) AS MaxStartDate
            FROM main.silver.humanresources_EmployeeDepartmentHistory
            WHERE StartDate <= CAST(current_timestamp() AS date)
              AND (EndDate IS NULL OR EndDate >= CAST(current_timestamp() AS date))
            GROUP BY BusinessEntityID
        ) m
          ON m.BusinessEntityID = edh2.BusinessEntityID
         AND m.MaxStartDate     = edh2.StartDate
    ) cur
      ON cur.BusinessEntityID = e.BusinessEntityID
    LEFT JOIN main.silver.humanresources_Department d
      ON d.DepartmentID = cur.DepartmentID
    LEFT JOIN main.silver.humanresources_Shift s
      ON s.ShiftID = cur.ShiftID;
END;
