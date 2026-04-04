import java.util.Scanner;

public class DBApp {

    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                printMenu();
                String input = scanner.nextLine().trim().toLowerCase();

                switch (input) {
                    case "1" -> TransportMeansApp.run();
                    case "2" -> CarRacingApp.run();
                    case "3" -> HotelBooking.run();
                    case "4" -> OrganizationStructure.run();
                    case "0" -> {
                        System.out.println("Выход");
                        return;
                    }
                    default -> {
                        System.out.println("Неизвестный выбор: " + input);
                        System.out.println("Варианты: 1 (Транспортные средства), 2 (Автомобильные гонки), 3 (Бронирование отелей), 4 (Структура организации), 0 (exit)");
                    }
                }

                System.out.println();
            }
        }
    }

    private static void printMenu() {
        System.out.println("Выберите кейс:");
        System.out.println("1 - Транспортные средства");
        System.out.println("2 - Автомобильные гонки");
        System.out.println("3 - Бронирование отелей");
        System.out.println("4 - Структура организации");
        System.out.println("0 - exit");
        System.out.print("Введите 1, 2, 3, 4 или 0: ");
    }
}
