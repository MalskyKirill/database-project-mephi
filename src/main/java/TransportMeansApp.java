import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class TransportMeansApp {
    private static final String TRANSPORT_URL = "jdbc:mysql://localhost:3306/transport";
    private static final String USER = "root";
    private static final String PASSWORD = "secret";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(TRANSPORT_URL, USER, PASSWORD)) {

            runSqlScript(connection, "schema1.sql");

            String queryTaskFirst = """
                Select v.maker, v.model
                From Vehicle as v
                Join Motorcycle as m On v.model = m.model
                Where m.horsepower > 150 And m.price < 20000 And m.type = 'Sport'
                Order by m.horsepower desc
                """;

            String queryTaskSecond = """
                Select v.maker, v.model, c.horsepower, c.engine_capacity, 'Car' as vehicle_type
                From Vehicle as v
                Join Car as c on c.model = v.model
                Where c.horsepower > 150 and c.engine_capacity < 3 and c.price < 35000
                Union all
                Select v.maker, v. model, m.horsepower, m.engine_capacity, 'Moto' as vehicle_type
                From Vehicle as v
                Join Motorcycle as m on m.model = v.model
                Where m.horsepower > 150 and m.engine_capacity < 1.5 and m.price < 20000
                Union all
                Select v.maker, v.model, Null as horsepower, Null as engine_capacity, 'Bicycle' as vehicle_type
                From Vehicle as v
                Join Bicycle as b on b.model = v.model
                Where b.gear_count > 18 and b.price < 4000
                Order by horsepower DESC
                """;

            System.out.println("Задача 1:");
            executeQueryFirstTask(connection, queryTaskFirst);

            System.out.println("------------------------------");

            System.out.println("Задача 2:");
            executeQuerySecondTask(connection, queryTaskSecond);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void executeQuerySecondTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-10s %-12s %-12s %-18s %-15s%n",
                    "maker", "model", "horsepower", "engine_capacity", "vehicle_type");
                while (rs.next()) {
                    Integer horsepower = (Integer) rs.getObject("horsepower");
                    BigDecimal engineCapacity = rs.getBigDecimal("engine_capacity");

                    System.out.printf("%-10s %-12s %-12s %-18s %-15s%n",
                        rs.getString("maker"),
                        rs.getString("model"),
                        horsepower != null ? horsepower : "NULL",
                        engineCapacity != null ? engineCapacity : "NULL",
                        rs.getString("vehicle_type"));
                }
            }
        }
    }

    private static void executeQueryFirstTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-10s %-12s%n", "maker", "model");
                while (rs.next()) {
                    System.out.printf("%-10s %-12s%n"
                        , rs.getString("maker")
                        , rs.getString("model"));
                }
            }
        }
    }

    private static void runSqlScript(Connection connection, String resource) throws IOException, SQLException {
        InputStream inputStream = TransportMeansApp.class.getResourceAsStream(resource); // загружаем схему в виде потока

        if (inputStream == null) {
            throw new IOException("Файл не найден в resources: " + resource);
        }

        StringBuilder sql = new StringBuilder(); // создаем билдер

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }
                sql.append(line).append("\n");
            }

            String[] statements = sql.toString().split(";");

            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                for (String stmt : statements) {
                    String statementTrim = stmt.trim();
                    if (!statementTrim.isEmpty()) {
                        statement.execute(statementTrim);
                    }
                }
                connection.commit();

            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }
}
