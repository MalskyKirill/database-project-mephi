import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class DatabaseConfig {
    public static final String RESOURCE = "app.properties";
    public static final Properties PROPERTIES = loadProp();

    public static String getOrThrow(String propertyKey) {
        String valueFromProperties = PROPERTIES.getProperty(propertyKey);
        if (valueFromProperties != null && !valueFromProperties.isBlank()) {
            return valueFromProperties;
        }

        throw new IllegalStateException(
            "Не задана настройка. Укажите параметр " + propertyKey + "' в " + RESOURCE);
    }

    private static Properties loadProp() {
        Properties properties = new Properties();
        try (InputStream inputStream = DatabaseConfig.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Не удалось загрузить " + RESOURCE, e);
        }

        return properties;
    }
}
