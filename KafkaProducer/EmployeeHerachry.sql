WITH RECURSIVE EmployeeHierarchy AS (
    -- Anchor member: select top-level managers (those with no ManagerId)
    SELECT
        Id,
        Name,
        ManagerId,
        Name AS Hierarchy
    FROM
        Employee
    WHERE
        ManagerId IS NULL

    UNION ALL

    -- Recursive member: select employees and concatenate hierarchy
    SELECT
        e.Id,
        e.Name,
        e.ManagerId,
        CONCAT(eh.Hierarchy, ' -> ', e.Name) AS Hierarchy
    FROM
        Employee e
    INNER JOIN
        EmployeeHierarchy eh ON e.ManagerId = eh.Id
)

-- Final select: get the full hierarchy
SELECT
    Id,
    Name,
    Hierarchy
FROM
    EmployeeHierarchy
ORDER BY
    Hierarchy;
