import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class SqlScriptRunner {

    private SqlScriptRunner() {
    }

    public static void runSqlScript(Class<?> resourceOwner, Connection connection, String resource)
        throws IOException, SQLException {
        InputStream inputStream = resourceOwner.getResourceAsStream(resource);

        if (inputStream == null) {
            throw new IOException("Файл не найден в resources: " + resource);
        }

        StringBuilder sql = new StringBuilder();

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
