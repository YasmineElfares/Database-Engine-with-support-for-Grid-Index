import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Index extends Vector implements Serializable{ //index itself is a vector of vectors that stores ranges
	
	Vector columnNames;
	String tableName;
	Vector buckets;
	Hashtable bucketOF;

	public Index(String tableName) {
		super();
		this.tableName=tableName;
		this.buckets=new Vector();
		this.columnNames = new Vector();
		this.bucketOF= new Hashtable();
	}

	@Override
	public String toString() {
		String res = "Index [columnNames=" + columnNames + ", tableName=" + tableName + ", buckets=" + buckets + ", bucketOF="
				+ bucketOF + "]";
		
		res += "\n";
		for (int i=0;i<this.size();i++) {
			res+= this.get(i).toString();
			res+= "\n";
		}
		
		return res;
	}
	
	
}
	 