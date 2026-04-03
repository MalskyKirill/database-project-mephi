import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

public final class HotelBooking {
    private static final String BOOKING_URL = "jdbc:mysql://localhost:3306/booking";
    private static final String USER = "root";
    private static final String PASSWORD = "secret";

    public static void run() {
        {
            try (Connection connection = DriverManager.getConnection(BOOKING_URL, USER, PASSWORD)) {
                SqlScriptRunner.runSqlScript(HotelBooking.class, connection, "schema3.sql");

                String queryTaskFirst = """
                    SELECT
                        c.name,
                        c.email,
                        c.phone,
                        COUNT(b.ID_booking) AS total_bookings,
                        GROUP_CONCAT(DISTINCT h.name ORDER BY h.name SEPARATOR ', ') AS hotels,
                        ROUND(AVG(DATEDIFF(b.check_out_date, b.check_in_date)), 4) AS avg_stay_days
                    FROM Customer c
                    JOIN Booking b ON c.ID_customer = b.ID_customer
                    JOIN Room r ON b.ID_room = r.ID_room
                    JOIN Hotel h ON r.ID_hotel = h.ID_hotel
                    GROUP BY c.ID_customer, c.name, c.email, c.phone
                    HAVING COUNT(b.ID_booking) > 2
                       AND COUNT(DISTINCT h.ID_hotel) > 1
                    ORDER BY total_bookings DESC;
                    """;

                String queryTaskSecond = """
                    SELECT
                        c.ID_customer,
                        c.name,
                        COUNT(b.ID_booking) AS total_bookings,
                        SUM(r.price) AS total_spent,
                        COUNT(DISTINCT h.ID_hotel) AS unique_hotels
                    FROM Customer c
                    JOIN Booking b ON c.ID_customer = b.ID_customer
                    JOIN Room r ON b.ID_room = r.ID_room
                    JOIN Hotel h ON r.ID_hotel = h.ID_hotel
                    GROUP BY c.ID_customer, c.name
                    HAVING COUNT(b.ID_booking) > 2
                       AND COUNT(DISTINCT h.ID_hotel) > 1
                       AND SUM(r.price) > 500
                    ORDER BY total_spent ASC
                    """;

                String queryTaskThird = """
                    WITH hotel_categories AS (
                        SELECT
                            h.ID_hotel,
                            h.name AS hotel_name,
                            CASE
                                WHEN AVG(r.price) < 175 THEN 'Дешевый'
                                WHEN AVG(r.price) <= 300 THEN 'Средний'
                                ELSE 'Дорогой'
                            END AS hotel_type
                        FROM Hotel h
                        JOIN Room r ON h.ID_hotel = r.ID_hotel
                        GROUP BY h.ID_hotel, h.name
                    )
                    SELECT
                        c.ID_customer,
                        c.name,
                        CASE
                            WHEN MAX(CASE WHEN hc.hotel_type = 'Дорогой' THEN 1 ELSE 0 END) = 1 THEN 'Дорогой'
                            WHEN MAX(CASE WHEN hc.hotel_type = 'Средний' THEN 1 ELSE 0 END) = 1 THEN 'Средний'
                            ELSE 'Дешевый'
                        END AS preferred_hotel_type,
                        GROUP_CONCAT(DISTINCT hc.hotel_name ORDER BY hc.hotel_name SEPARATOR ', ') AS visited_hotels
                    FROM Customer c
                    JOIN Booking b ON c.ID_customer = b.ID_customer
                    JOIN Room r ON b.ID_room = r.ID_room
                    JOIN hotel_categories hc ON r.ID_hotel = hc.ID_hotel
                    GROUP BY c.ID_customer, c.name
                    ORDER BY
                        CASE
                            WHEN MAX(CASE WHEN hc.hotel_type = 'Дешевый' THEN 1 ELSE 0 END) = 1
                                 AND MAX(CASE WHEN hc.hotel_type = 'Средний' THEN 1 ELSE 0 END) = 0
                                 AND MAX(CASE WHEN hc.hotel_type = 'Дорогой' THEN 1 ELSE 0 END) = 0 THEN 1
                            WHEN MAX(CASE WHEN hc.hotel_type = 'Дорогой' THEN 1 ELSE 0 END) = 1 THEN 3
                            ELSE 2
                        END,
                        c.ID_customer;
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
    }

    private static void executeQueryThirdTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-12s %-20s %-22s %-35s%n",
                    "ID_customer", "name", "preferred_hotel_type", "visited_hotels");

                while (rs.next()) {
                    System.out.printf("%-12d %-20s %-22s %-35s%n",
                        rs.getInt("ID_customer"),
                        rs.getString("name"),
                        rs.getString("preferred_hotel_type"),
                        rs.getString("visited_hotels"));
                }
            }
        }
    }

    private static void executeQuerySecondTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-12s %-20s %-18s %-15s %-15s%n",
                    "ID_customer", "name", "total_bookings", "total_spent", "unique_hotels");

                while (rs.next()) {
                    Long totalBookings = rs.getObject("total_bookings", Long.class);
                    BigDecimal totalSpent = rs.getBigDecimal("total_spent");
                    Long uniqueHotels = rs.getObject("unique_hotels", Long.class);

                    System.out.printf("%-12s %-20s %-18s %-15s %-15s%n",
                        rs.getInt("ID_customer"),
                        rs.getString("name"),
                        totalBookings != null ? totalBookings : "NULL",
                        totalSpent != null ? totalSpent : "NULL",
                        uniqueHotels != null ? uniqueHotels : "NULL");
                }
            }
        }
    }

    private static void executeQueryFirstTask(Connection connection, String query) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                System.out.printf("%-20s %-30s %-15s %-15s %-40s %-15s%n",
                    "name", "email", "phone", "total_bookings", "hotels", "avg_stay_days");

                while (rs.next()) {
                    Long totalBookings = rs.getObject("total_bookings", Long.class);
                    BigDecimal avgStayDays = rs.getBigDecimal("avg_stay_days");

                    System.out.printf("%-20s %-30s %-15s %-15s %-40s %-15s%n",
                        rs.getString("name"),
                        rs.getString("email"),
                        rs.getString("phone"),
                        totalBookings != null ? totalBookings : "NULL",
                        rs.getString("hotels"),
                        avgStayDays != null ? avgStayDays : "NULL");
                }
            }
        }
    }
}
