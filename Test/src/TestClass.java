import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;


public class TestClass {
public static void main(String args[])
{
	/* ArrayList<Integer> list = new ArrayList<Integer>();
     for (int i=1; i<=25; i++) {
         list.add(new Integer(i));
     }
     Collections.shuffle(list);
     for (int i=0; i<10; i++) {
         System.out.println(list.get(i));
     }*/
	
	Random rand= new Random();
	int num=rand.nextInt(10)+1;
	System.out.println(num);
}
}
