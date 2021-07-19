import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class Tuple extends Vector implements Comparable, Serializable{
//	Vector v;
	
	public Tuple() {
//		this.v = new Vector();
	}
	
	
	@Override
//	public String toString() {
//		return this.toString();
//	}


	public int compareTo(Object o) {
		Tuple obj = (Tuple)o;
		Object x = this.get(0);
		Object y = obj.get(0);
		

		int res=2222;
		String type = x.getClass().getName();
					
			switch(type) {
			case "java.lang.Integer": 
				if((int)x< (int)y) {
					res=-1;
				}
				else 
				{if ((int)x > (int)y)
					res=1;
				else 
					res=0;
				}
				
				break;
				
			case "java.lang.String": 
				res=(((String)x).compareToIgnoreCase((String)y)); {
					
				}break;
				
			case "java.lang.Double": 
				if((double)x< (double)y ) {
					res=-1;
				}
				else if ((double)x> (double)y ){
				res=1;
				}
				else
					res=0;
				break;

			case "java.util.Date": 
				
//				String xx=x+"";
//				String yy=y+"";
				Date indate;
				indate = (Date)x;

						
Date date2=(Date)y;

res= indate.compareTo(date2);
					
				break;
			}
			

			return res;
}
	}
	
