import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable{
	
	Vector ranges;
	int nPages;
	Vector sizes;
	ArrayList columns;
	ArrayList NOverflows;
	Vector overFlowRanges;
	Vector overFlowSizes;
	Vector indices;
	
	
	
	public Table() {
		
		this.ranges = new Vector();
		this.nPages = nPages;
		this.sizes = new Vector();
		this.columns = new ArrayList();
		this.NOverflows = new ArrayList();
		this.overFlowRanges = new Vector();
		this.overFlowSizes = new Vector();
		indices = new Vector();
	}




	@Override
	public String toString() {
		return "Table [ranges=" + ranges + ", nPages=" + nPages + ", sizes=" + sizes + ", columns=" + columns
				+ ", NOverflows=" + NOverflows + ", overFlowRanges=" + overFlowRanges + ", overFlowSizes="
				+ overFlowSizes + "]";
	}





	
	
	
}
