import java.io.File;
import java.util.Scanner;
import java.io.FileNotFoundException;

public class Verify {
    public static void main(String args[]) throws FileNotFoundException {
        File folder = new File("./sample_student_scores/input");
        File[] listOfFiles = folder.listFiles();

        int count_math = 0;
        int sum_math = 0;
        int square_sum_math = 0;
        int count_hist = 0;
        int sum_hist = 0;
        int square_sum_hist = 0;

        for (File file : listOfFiles) {
            if (file.isFile()) {
                //System.out.println(file.getName());
                Scanner input = new Scanner(file);
                while(input.hasNext()) {
                    String student_name = input.next();
                    String class_name = input.next();
                    int score = Integer.parseInt(input.next());
                    if (class_name.equals("History")){
                        count_hist++;
                        sum_hist += score;
                        square_sum_hist += score*score;
                    } else if (class_name.equals("Math")){
                        count_math++;
                        sum_math += score;
                        square_sum_math += score*score;
                    }
                }

            }
        }
        System.out.println("History " + count_hist + " " + sum_hist + " " + square_sum_hist);
        System.out.println("Math " + count_math + " " + sum_math + " " + square_sum_math);

        double average_hist = (double)(sum_hist)/count_hist;
        double square_sum_average_hist = (double)(square_sum_hist)/count_hist;
        double standard_deviation_hist = Math.sqrt(square_sum_average_hist-
            average_hist*average_hist);
        double cutoff_hist = average_hist-standard_deviation_hist;

        double average_math = (double)(sum_math)/count_math;
        double square_sum_average_math = (double)(square_sum_math)/count_math;
        double standard_deviation_math = Math.sqrt(square_sum_average_math-
            average_math*average_math);
        double cutoff_math = average_math-standard_deviation_math;

        for (File file : listOfFiles) {
            if (file.isFile()) {
                Scanner input = new Scanner(file);
                while(input.hasNext()) {
                    String student_name = input.next();
                    String class_name = input.next();
                    int score = Integer.parseInt(input.next());
                    if (class_name.equals("History")){
                        if (score < 50)//cutoff_hist)
                        System.out.println(student_name+ " History");
                    } else if (class_name.equals("Math")){
                        if (score < 50)//cutoff_hist)
                        System.out.println(student_name+ " Math");
                    }
                }

            }
        }
    }
}
