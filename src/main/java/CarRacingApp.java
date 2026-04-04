import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

public final class CarRacingApp {

    public static void run() {
        String dbUrl = DatabaseConfig.getOrThrow("db.racing.url");
        String dbUser = DatabaseConfig.getOrThrow("db.user");
        String dbPassword = DatabaseConfig.getOrThrow("db.password");

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            SqlScriptRunner.runSqlScript(CarRacingApp.class, connection, "schema2.sql");
            String queryTaskFirst = """
                WITH car_stats AS (
                    SELECT
                        c.name AS car_name,
                        c.class AS car_class,
                        AVG(r.position) AS average_position,
                        COUNT(r.race) AS race_count
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.name, c.class
                ),
                ranked AS (
                    SELECT
                        car_name,
                        car_class,
                        average_position,
                        race_count,
                        DENSE_RANK() OVER (
                            PARTITION BY car_class
                            ORDER BY average_position
                        ) AS rk
                    FROM car_stats
                )
                SELECT
                    r.car_name,
                    r.car_class,
                    ROUND(r.average_position, 4) AS average_position,
                    r.race_count
                FROM ranked r
                JOIN Classes cl ON cl.class = r.car_class
                WHERE r.rk = 1
                ORDER BY r.average_position, cl.engineSize;
                """;

            String queryTaskSecond = """
                WITH car_stats AS (
                    SELECT
                        c.name AS car_name,
                        c.class AS car_class,
                        AVG(r.position) AS average_position,
                        COUNT(r.race) AS race_count
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.name, c.class
                )
                SELECT
                    cs.car_name,
                    cs.car_class,
                    ROUND(cs.average_position, 4) AS average_position,
                    cs.race_count,
                    cl.country AS car_country
                FROM car_stats cs
                JOIN Classes cl ON cl.class = cs.car_class
                ORDER BY cs.average_position, cs.car_name
                LIMIT 1;
                """;

            String queryTaskThird = """
                WITH car_stats AS (
                    SELECT
                        c.name AS car_name,
                        c.class AS car_class,
                        AVG(r.position) AS average_position,
                        COUNT(r.race) AS race_count
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.name, c.class
                ),
                class_stats AS (
                    SELECT
                        c.class AS car_class,
                        AVG(r.position) AS class_average_position,
                        COUNT(r.race) AS total_races
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.class
                ),
                best_classes AS (
                    SELECT car_class, total_races
                    FROM class_stats
                    WHERE class_average_position = (
                        SELECT MIN(class_average_position)
                        FROM class_stats
                    )
                )
                SELECT
                    cs.car_name,
                    cs.car_class,
                    ROUND(cs.average_position, 4) AS average_position,
                    cs.race_count,
                    cl.country AS car_country,
                    bc.total_races
                FROM car_stats cs
                JOIN best_classes bc ON bc.car_class = cs.car_class
                JOIN Classes cl ON cl.class = cs.car_class
                ORDER BY cs.average_position, cs.car_name;
                """;

            String queryTaskFourth = """
                WITH car_stats AS (
                    SELECT
                        c.name AS car_name,
                        c.class AS car_class,
                        AVG(r.position) AS average_position,
                        COUNT(r.race) AS race_count
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.name, c.class
                ),
                class_stats AS (
                    SELECT
                        car_class,
                        AVG(average_position) AS class_average_position,
                        COUNT(*) AS car_count
                    FROM car_stats
                    GROUP BY car_class
                    HAVING COUNT(*) >= 2
                )
                SELECT
                    cs.car_name,
                    cs.car_class,
                    ROUND(cs.average_position, 4) AS average_position,
                    cs.race_count,
                    cl.country AS car_country
                FROM car_stats cs
                JOIN class_stats cls ON cls.car_class = cs.car_class
                JOIN Classes cl ON cl.class = cs.car_class
                WHERE cs.average_position < cls.class_average_position
                ORDER BY cs.car_class, cs.average_position;
                """;

            String queryTaskFifth = """
                WITH car_stats AS (
                    SELECT
                        c.name AS car_name,
                        c.class AS car_class,
                        AVG(r.position) AS average_position,
                        COUNT(r.race) AS race_count
                    FROM Cars c
                    JOIN Results r ON r.car = c.name
                    GROUP BY c.name, c.class
                ),
                class_stats AS (
                    SELECT
                        car_class,
                        SUM(race_count) AS total_races,
                        SUM(CASE WHEN average_position >= 3.0 THEN 1 ELSE 0 END) AS low_position_count
                    FROM car_stats
                    GROUP BY car_class
                    HAVING SUM(CASE WHEN average_position >= 3.0 THEN 1 ELSE 0 END) > 0
                )
                SELECT
                    cs.car_name,
                    cs.car_class,
                    ROUND(cs.average_position, 4) AS average_position,
                    cs.race_count,
                    cl.country AS car_country,
                    cls.total_races,
                    cls.low_position_count
                FROM car_stats cs
                JOIN class_stats cls ON cls.car_class = cs.car_class
                JOIN Classes cl ON cl.class = cs.car_class
                WHERE cs.average_position > 3.0
                ORDER BY cls.low_position_count DESC, cs.car_class, cs.car_name;
                """;

            System.out.println("Задача 1:");
            executeQueryFirstTask(connection, queryTaskFirst);

            System.out.println("--------------");

            System.out.println("Задача 2:");
            executeQuerySecondTask(connection, queryTaskSecond);

            System.out.println("--------------");

            System.out.println("Задача 3:");
            executeQueryThirdTask(connection, queryTaskThird);

            System.out.println("--------------");

            System.out.println("Задача 4:");
            executeQueryFourthTask(connection, queryTaskFourth);

            System.out.println("--------------");

            System.out.println("Задача 5:");
            executeQueryFifthTask(connection, queryTaskFifth);


        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void executeQueryFifthTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-22s %-18s %-18s %-12s %-15s %-12s %-18s%n",
                    "car_name", "car_class", "average_position", "race_count",
                    "car_country", "total_races", "low_position_count");

                while (rs.next()) {
                    BigDecimal averagePosition = rs.getBigDecimal("average_position");
                    Long raceCount = rs.getObject("race_count", Long.class);
                    Long totalRaces = rs.getObject("total_races", Long.class);
                    Long lowPositionCount = rs.getObject("low_position_count", Long.class);

                    System.out.printf("%-22s %-18s %-18s %-12s %-15s %-12s %-18s%n",
                        rs.getString("car_name"),
                        rs.getString("car_class"),
                        averagePosition != null ? averagePosition : "NULL",
                        raceCount != null ? raceCount : "NULL",
                        rs.getString("car_country"),
                        totalRaces != null ? totalRaces : "NULL",
                        lowPositionCount != null ? lowPositionCount : "NULL");
                }
            }
        }
    }

    private static void executeQueryFourthTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-22s %-18s %-18s %-12s %-15s%n",
                    "car_name", "car_class", "average_position", "race_count", "car_country");

                while (rs.next()) {
                    BigDecimal averagePosition = rs.getBigDecimal("average_position");
                    Long raceCount = rs.getObject("race_count", Long.class);

                    System.out.printf("%-22s %-18s %-18s %-12s %-15s%n",
                        rs.getString("car_name"),
                        rs.getString("car_class"),
                        averagePosition != null ? averagePosition : "NULL",
                        raceCount != null ? raceCount : "NULL",
                        rs.getString("car_country"));
                }
            }
        }
    }

    private static void executeQueryThirdTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-22s %-18s %-18s %-12s %-15s %-12s%n",
                    "car_name", "car_class", "average_position", "race_count", "car_country", "total_races");

                while (rs.next()) {
                    BigDecimal averagePosition = rs.getBigDecimal("average_position");
                    Long raceCount = rs.getObject("race_count", Long.class);
                    Long totalRaces = rs.getObject("total_races", Long.class);

                    System.out.printf("%-22s %-18s %-18s %-12s %-15s %-12s%n",
                        rs.getString("car_name"),
                        rs.getString("car_class"),
                        averagePosition != null ? averagePosition : "NULL",
                        raceCount != null ? raceCount : "NULL",
                        rs.getString("car_country"),
                        totalRaces != null ? totalRaces : "NULL");
                }
            }
        }
    }

    private static void executeQuerySecondTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-22s %-18s %-18s %-12s %-15s%n",
                    "car_name", "car_class", "average_position", "race_count", "car_country");

                while (rs.next()) {
                    BigDecimal averagePosition = rs.getBigDecimal("average_position");
                    Long raceCount = rs.getObject("race_count", Long.class);

                    System.out.printf("%-22s %-18s %-18s %-12s %-15s%n",
                        rs.getString("car_name"),
                        rs.getString("car_class"),
                        averagePosition != null ? averagePosition : "NULL",
                        raceCount != null ? raceCount : "NULL",
                        rs.getString("car_country"));
                }
            }
        }
    }

    private static void executeQueryFirstTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-22s %-18s %-18s %-12s%n",
                    "car_name", "car_class", "average_position", "race_count");

                while (rs.next()) {
                    System.out.printf("%-22s %-18s %-18s %-12s%n",
                        rs.getString("car_name"),
                        rs.getString("car_class"),
                        rs.getBigDecimal("average_position"),
                        rs.getLong("race_count"));
                }
            }
        }
    }
}
