//AUTHOR:zengli (bookug)

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class TableGenerator
{
	public static int studentNum = 16000000;
	public static int teacherNum = studentNum / 2;
	public static int courseNum = teacherNum / 8;
	
	//NOTICE:here we set R much smaller
	public static int tableRScale = 100000;
	public static int tableSScale = 1000000;
	public static int ageScale = 100;

	public static int randR = 19920329;
	public static int randS = 1000000007;
	
	public void generateTableR()
	{
		System.out.println("to generate Table R...");
		File equal_output = new File("./DATA/table1R.dat");
		File not_equal_output = new File("./DATA/table2R.dat");
		Random rand = new Random(randR);
		
		try
		{
			OutputStreamWriter writer1 = new OutputStreamWriter(new FileOutputStream(equal_output),"utf-8");
			OutputStreamWriter writer2 = new OutputStreamWriter(new FileOutputStream(not_equal_output),"utf-8");
			
			for (int i = 0; i < tableRScale; i++)
			{
				int courseId = rand.nextInt(courseNum);
				int teacherId = rand.nextInt(teacherNum);
				String courseName = "course_" + courseId;
				String teacherName = "teacher_" + teacherId;
				writer1.write(courseName + "\t" + teacherName + "\n");
				
				String teacherAge = String.valueOf(rand.nextInt(ageScale));
				writer2.write(teacherAge + "\t" + teacherName + "\n");
				
			}
			writer1.close();
			writer2.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		System.out.println("finish generate Table R");
	}
	
	public void gererateTableS()
	{
		System.out.println("to generate Table S...");
		File equal_output = new File("./DATA/table1S.dat");
		File not_equal_output = new File("./DATA/table2S.dat");
		Random rand = new Random(randS);
		
		try
		{
			OutputStreamWriter writer1 = new OutputStreamWriter(new FileOutputStream(equal_output),"utf-8");
			OutputStreamWriter writer2 = new OutputStreamWriter(new FileOutputStream(not_equal_output),"utf-8");
			
			for (int i = 0; i < tableSScale; i++)
			{
				int courseId = rand.nextInt(courseNum);
				int studentId = rand.nextInt(studentNum);
				String courseName = "course_" + courseId;
				String studentName = "student_" + studentId;
				writer1.write(courseName + "\t" + studentName + "\n");
				
				String studentAge = String.valueOf(rand.nextInt(ageScale));
				writer2.write(studentAge + "\t" + studentName + "\n");
			}
			writer1.close();
			writer2.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		System.out.println("finish generate Table S");		
	}
	
	public static void main(String[] args)
	{
		TableGenerator tg = new TableGenerator();
		tg.generateTableR();
		tg.gererateTableS();
	}
}

