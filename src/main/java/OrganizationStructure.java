import java.io.IOException;
import java.sql.*;

public final class OrganizationStructure {
    public static void run() {
        String dbUrl = DatabaseConfig.getOrThrow("db.struct.url");
        String dbUser = DatabaseConfig.getOrThrow("db.user");
        String dbPassword = DatabaseConfig.getOrThrow("db.password");

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            SqlScriptRunner.runSqlScript(CarRacingApp.class, connection, "schema4.sql");

            String queryTaskFirst = """
                WITH RECURSIVE subordinates AS (
                    SELECT
                        e.EmployeeID,
                        e.Name,
                        e.ManagerID,
                        e.DepartmentID,
                        e.RoleID
                    FROM Employees e
                    WHERE e.EmployeeID = 1

                    UNION ALL

                    SELECT
                        e.EmployeeID,
                        e.Name,
                        e.ManagerID,
                        e.DepartmentID,
                        e.RoleID
                    FROM Employees e
                    INNER JOIN subordinates s
                        ON e.ManagerID = s.EmployeeID
                )
                SELECT
                    s.EmployeeID,
                    s.Name AS EmployeeName,
                    s.ManagerID,
                    d.DepartmentName,
                    r.RoleName,

                    (
                        SELECT GROUP_CONCAT(p.ProjectName ORDER BY p.ProjectName SEPARATOR ', ')
                        FROM Projects p
                        WHERE p.DepartmentID = s.DepartmentID
                    ) AS ProjectNames,

                    (
                        SELECT GROUP_CONCAT(t.TaskName ORDER BY t.TaskName SEPARATOR ', ')
                        FROM Tasks t
                        WHERE t.AssignedTo = s.EmployeeID
                    ) AS TaskNames

                FROM subordinates s
                LEFT JOIN Departments d
                    ON s.DepartmentID = d.DepartmentID
                LEFT JOIN Roles r
                    ON s.RoleID = r.RoleID
                ORDER BY s.Name;
                """;

            String queryTaskSecond = """
                WITH RECURSIVE subordinates AS (
                    SELECT
                        e.EmployeeID,
                        e.Name,
                        e.ManagerID,
                        e.DepartmentID,
                        e.RoleID
                    FROM Employees e
                    WHERE e.EmployeeID = 1

                    UNION ALL

                    SELECT
                        e.EmployeeID,
                        e.Name,
                        e.ManagerID,
                        e.DepartmentID,
                        e.RoleID
                    FROM Employees e
                    INNER JOIN subordinates s
                        ON e.ManagerID = s.EmployeeID
                ),
                project_agg AS (
                    SELECT
                        p.DepartmentID,
                        GROUP_CONCAT(p.ProjectName ORDER BY p.ProjectName SEPARATOR ', ') AS ProjectNames
                    FROM Projects p
                    GROUP BY p.DepartmentID
                ),
                task_agg AS (
                    SELECT
                        t.AssignedTo AS EmployeeID,
                        GROUP_CONCAT(t.TaskName ORDER BY t.TaskName SEPARATOR ', ') AS TaskNames,
                        COUNT(t.TaskID) AS TotalTasks
                    FROM Tasks t
                    GROUP BY t.AssignedTo
                ),
                subordinate_count AS (
                    SELECT
                        e.ManagerID,
                        COUNT(e.EmployeeID) AS TotalSubordinates
                    FROM Employees e
                    WHERE e.ManagerID IS NOT NULL
                    GROUP BY e.ManagerID
                )
                SELECT
                    s.EmployeeID,
                    s.Name AS EmployeeName,
                    s.ManagerID,
                    d.DepartmentName,
                    r.RoleName,
                    pa.ProjectNames,
                    ta.TaskNames,
                    COALESCE(ta.TotalTasks, 0) AS TotalTasks,
                    COALESCE(sc.TotalSubordinates, 0) AS TotalSubordinates
                FROM subordinates s
                LEFT JOIN Departments d
                    ON s.DepartmentID = d.DepartmentID
                LEFT JOIN Roles r
                    ON s.RoleID = r.RoleID
                LEFT JOIN project_agg pa
                    ON s.DepartmentID = pa.DepartmentID
                LEFT JOIN task_agg ta
                    ON s.EmployeeID = ta.EmployeeID
                LEFT JOIN subordinate_count sc
                    ON s.EmployeeID = sc.ManagerID
                ORDER BY s.Name;
                """;

            String queryTaskThird = """
                WITH RECURSIVE manager_tree AS (
                    SELECT
                        e.EmployeeID AS ManagerEmployeeID,
                        s.EmployeeID AS SubordinateID
                    FROM Employees e
                    JOIN Roles r
                        ON e.RoleID = r.RoleID
                    JOIN Employees s
                        ON s.ManagerID = e.EmployeeID
                    WHERE r.RoleName = 'Менеджер'

                    UNION ALL

                    SELECT
                        mt.ManagerEmployeeID,
                        e.EmployeeID AS SubordinateID
                    FROM manager_tree mt
                    JOIN Employees e
                        ON e.ManagerID = mt.SubordinateID
                ),
                project_agg AS (
                    SELECT
                        p.DepartmentID,
                        GROUP_CONCAT(p.ProjectName ORDER BY p.ProjectName SEPARATOR ', ') AS ProjectNames
                    FROM Projects p
                    GROUP BY p.DepartmentID
                ),
                task_agg AS (
                    SELECT
                        t.AssignedTo AS EmployeeID,
                        GROUP_CONCAT(t.TaskName ORDER BY t.TaskName SEPARATOR ', ') AS TaskNames
                    FROM Tasks t
                    GROUP BY t.AssignedTo
                )
                SELECT
                    e.EmployeeID,
                    e.Name AS EmployeeName,
                    e.ManagerID,
                    d.DepartmentName,
                    r.RoleName,
                    pa.ProjectNames,
                    ta.TaskNames,
                    COUNT(DISTINCT mt.SubordinateID) AS TotalSubordinates
                FROM Employees e
                JOIN Roles r
                    ON e.RoleID = r.RoleID
                LEFT JOIN Departments d
                    ON e.DepartmentID = d.DepartmentID
                LEFT JOIN project_agg pa
                    ON e.DepartmentID = pa.DepartmentID
                LEFT JOIN task_agg ta
                    ON e.EmployeeID = ta.EmployeeID
                LEFT JOIN manager_tree mt
                    ON e.EmployeeID = mt.ManagerEmployeeID
                WHERE r.RoleName = 'Менеджер'
                GROUP BY
                    e.EmployeeID,
                    e.Name,
                    e.ManagerID,
                    d.DepartmentName,
                    r.RoleName,
                    pa.ProjectNames,
                    ta.TaskNames
                HAVING COUNT(DISTINCT mt.SubordinateID) > 0
                ORDER BY e.Name;
                """;


            System.out.println("Задача 1:");
            executeQueryFirstTask(connection, queryTaskFirst);

            System.out.println("--------------");

            System.out.println("Задача 2:");
            executeQuerySecondTask(connection, queryTaskSecond);

            System.out.println("--------------");

            System.out.println("Задача 3:");
            executeQueryThirdTask(connection, queryTaskThird);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void executeQueryThirdTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    Integer managerId = rs.getObject("ManagerID", Integer.class);
                    String projectNames = rs.getString("ProjectNames");
                    String taskNames = rs.getString("TaskNames");

                    System.out.println("------------------------------------------------------------");
                    System.out.printf("EmployeeID        : %d%n", rs.getInt("EmployeeID"));
                    System.out.printf("EmployeeName      : %s%n", rs.getString("EmployeeName"));
                    System.out.printf("ManagerID         : %s%n", managerId != null ? managerId : "NULL");
                    System.out.printf("DepartmentName    : %s%n", rs.getString("DepartmentName"));
                    System.out.printf("RoleName          : %s%n", rs.getString("RoleName"));
                    System.out.printf("ProjectNames      : %s%n", projectNames != null ? projectNames : "NULL");
                    System.out.printf("TaskNames         : %s%n", taskNames != null ? taskNames : "NULL");
                    System.out.printf("TotalSubordinates : %d%n", rs.getInt("TotalSubordinates"));
                }
                System.out.println("------------------------------------------------------------");
            }
        }
    }

    private static void executeQuerySecondTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf(
                    "%-12s %-25s %-12s %-22s %-28s %-20s %-60s %-12s %-18s%n",
                    "EmployeeID", "EmployeeName", "ManagerID", "DepartmentName", "RoleName", "ProjectNames", "TaskNames",
                    "TotalTasks", "TotalSubordinates"
                );

                while (rs.next()) {
                    Integer managerId = rs.getObject("ManagerID", Integer.class);
                    String projectNames = rs.getString("ProjectNames");
                    String taskNames = rs.getString("TaskNames");

                    System.out.printf(
                        "%-12d %-25s %-12s %-22s %-28s %-20s %-60s %-12d %-18d%n",
                        rs.getInt("EmployeeID"),
                        rs.getString("EmployeeName"),
                        managerId != null ? managerId : "NULL",
                        rs.getString("DepartmentName"),
                        rs.getString("RoleName"),
                        projectNames != null ? projectNames : "NULL",
                        taskNames != null ? taskNames : "NULL",
                        rs.getInt("TotalTasks"),
                        rs.getInt("TotalSubordinates")
                    );
                }
            }
        }
    }

    private static void executeQueryFirstTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-12s %-25s %-12s %-22s %-28s %-20s %-60s%n",
                    "EmployeeID", "EmployeeName", "ManagerID", "DepartmentName", "RoleName", "ProjectNames", "TaskNames"
                );

                while (rs.next()) {
                    Integer managerId = (Integer) rs.getObject("ManagerID");
                    String projectNames = rs.getString("ProjectNames");
                    String taskNames = rs.getString("TaskNames");

                    System.out.printf(
                        "%-12d %-25s %-12s %-22s %-28s %-20s %-60s%n",
                        rs.getInt("EmployeeID"),
                        rs.getString("EmployeeName"),
                        managerId != null ? managerId : "NULL",
                        rs.getString("DepartmentName"),
                        rs.getString("RoleName"),
                        projectNames != null ? projectNames : "NULL",
                        taskNames != null ? taskNames : "NULL"
                    );
                }
            }
        }
    }
}
