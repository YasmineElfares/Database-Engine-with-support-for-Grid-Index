import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

public class test extends Vector {
public static void main(String[] args) throws ParseException {
	
	Vector v1= new Vector();
	v1.add(1);
	v1.add(2);
	
	Tuple t = new Tuple();
	t.add(2);
	t.add("a");
	
	
	Tuple t2 = new Tuple();
	t2.add(2);
	t2.add("b");
	
	System.out.println(1 ^ 3);
	
	page p = new page();
	p.add(t);
	p.add(t2);
	
	
//	System.out.println(Collections.binarySearch(p,t2));
	
	Iterator<Integer> it = v1.iterator();
//	Vector v2= new Vector();
//	v2.add(2);
//	v2.add(1);
//	
//	String[] batates = {"b","a"};
//	String[] b2 = {"a","b"};
//	Arrays.sort(batates);
////	for(int i=0;i<batates.length;i++)
////		System.out.println(batates[i]);
//	
//	System.out.println(batates.equals(b2));
//	Tuple tuple1 = new Tuple();
//	tuple1.add(50);
//	tuple1.add("a");
//	
//	Tuple tuple2 = new Tuple();
//	tuple2.add(50);
//	tuple2.add("b");
//	
//	page p = new page();
//	p.add(tuple1);
//	p.add(tuple2);
//	
//	System.out.println(Collections.binarySearch(p, tuple2));
//	if(false) {
//		System.out.println("saba7 el fol");
//	}
//	else {
//		System.out.println("di 7aga f kemmet el batates");
//	}
	
	
	
}
}
