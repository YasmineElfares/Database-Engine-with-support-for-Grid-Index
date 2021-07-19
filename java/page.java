import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class page extends Vector implements Serializable {
//	Vector v; // tuples 
	
//	int nTuples;
//	String table; // name of table
	
	public page() {
		super();
//		this.v = new Vector();
//		this.overFlowRanges = new Vector();
//		this.overFlowSizes= new Vector();
//		this.nTuples=0;
//		this.table=table;
	}
	
	
	
	@Override
	public String toString() {
		return "page: " + super.toString() + "\n";
	}



	public boolean containsKey(Tuple t) {
		
		for (int i=0;i<this.size();i++) {
		
			Tuple tmp =(Tuple)this.get(i);
			if(tmp.compareTo(t)==0)
				return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		page p = new page();
		Tuple t1 = new Tuple();
		t1.add(2);
		t1.add("Ahme");
		p.add(t1);
		
		Tuple t2 = new Tuple();
		t2.add(1);
		t2.add("Mark");
		
		System.out.println(p.containsKey(t2));
		
		
		
	}
	
	

}
