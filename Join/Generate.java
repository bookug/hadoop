import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Generate {
    public static void main(String args[]) {
        int studentID = 0;
        String studentName;
        String school;
        String tuple1;
        String tuple2;

        Random random = new Random();
        double GPA = 0;
        String[] SchoolSet = new String[]{"Physics", "Math", "Computer Science"};
        String[] firstName = new String[]{"Zhang", "Zhao", "Gao", "Wu", "Zeng", "Li", "Jiang"};
        String[] lastName = new String[]{"Baohua", "Zeming", "Jun", "Li", "Ruiyang", "Zijin", "Jing", "Weiming", "Youwei", "Qijie"};

        FileWriter writer1;
        FileWriter writer2;
        try {
            writer1 = new FileWriter("d:/table1new.txt");
            writer2 = new FileWriter("d:/table2new.txt");



            for (int i = 0; i < 9999; i++) {
                studentID = 1600000 + i;
                studentName = firstName[random.nextInt(firstName.length)] + lastName[random.nextInt(lastName.length)];
                school = SchoolSet[random.nextInt(SchoolSet.length)];
                GPA = ((double) (random.nextInt(30) + 20)) / 10;

                tuple1 = studentID + "\t" + GPA +"\r\n";
                tuple2 = studentID + "\t" + studentName + "\t" + school + "\r\n";
                writer1.write(tuple1);
                System.out.println(tuple1);
                writer2.write(tuple2);
               writer1.flush();
                writer2.flush();
            }
             writer1.close();
            writer2.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}

