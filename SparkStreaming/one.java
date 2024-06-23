import java.util.Arrays;
import java.util.Comparator;
import java.util.Scanner;

class HelloWorld {


    private static class CustomComparator implements Comparator<Integer> {
        Scanner scanner = new Scanner(System.in);
        @Override
        public int compare(Integer a, Integer b) {
            String ab = a + "" + b;
            String ba = b + "" + a;
             int age = scanner.nextInt();
            System.out.println("=> "+ab);
            System.out.println("=> "+ba);
            return ba.compareTo(ab); // Reverse order to get the largest number
        }
    }

    public static void main(String[] args) {
       int[] myArray = {3, 30, 34, 5, 9};
       Integer[] integers = Arrays.stream(myArray).boxed().toArray(Integer[]::new); 
       for (int a : integers) {
           System.out.println("=> "+a);
       }
       Arrays.sort(integers, new CustomComparator());
    }
}
