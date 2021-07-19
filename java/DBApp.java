import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;

public class DBApp implements DBAppInterface {
	static int maxRows;
	static int maxBuckets;
	static String lastPage;
//	static String lastEdited;
//	static String lastEdited2;
	@Override
	public void init() {
		int[] config = config();
		maxRows = config[0];
		maxBuckets = config[1];
		lastPage = "";
//		lastEdited= "";
//		lastEdited2="";

	}

	public static int[] config() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {

		}
		try {
			prop.load(is);
		} catch (IOException ex) {

		}
		int a = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		int b = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
		int[] res = { a, b };
		return res;
	}

	
	
	public static void updateCSV(String tableName,Vector columnNames) {
		String row = "";
		BufferedReader csvReader;
		Vector lines = new Vector();
		try {
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if(data[0].equalsIgnoreCase(tableName) && columnNames.contains(data[1])) {
					data[4]="True";
				}
				String tmp = "";
				for(int i=0;i<data.length-1;i++)
				{
					tmp+= data[i] +",";
				}
				tmp+= data[data.length-1];
				lines.add(tmp);
			}
			csvReader.close();
			
			File f1 = new File("src/main/resources/metadata.csv");
			f1.delete();
			FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv");
			
			
		for(int i = 0;i<lines.size();i++) {
		csvWriter.append((String) lines.get(i) + "\n");
		
		}
		csvWriter.flush();
		csvWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void writeToCsv(String args) throws IOException {

		FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
//    		String res="";
//    		for (int i=0; i<args.length; i++) {
//    			if (i==args.length-1)
//    				res+=args[i];
//    			else
//    				res+=args[i]+",";
//    		}
		
		csvWriter.append(args);
		csvWriter.append("\n");
		
		csvWriter.flush();
		csvWriter.close();
	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		try {
			if (checkTableDuplicate(tableName) == true)
				throw new DBAppException();
			else {
//			 page p= new page(); 
				Table t = new Table();

				Enumeration<String> keys = colNameType.keys();
				ArrayList columnNames = new ArrayList<>();

				t.columns.add(clusteringKey);

				while (keys.hasMoreElements()) {
					String tmp = keys.nextElement().toString();
					columnNames.add(tmp);
					if (!(tmp.equalsIgnoreCase(clusteringKey)))
						t.columns.add(tmp);

				}

//	         t.sizes.add(0);

				for (int i = 0; i < columnNames.size(); i++) {

					String res = tableName + "," + columnNames.get(i) + ",";
					res += colNameType.get(columnNames.get(i)) + ",";

					if (clusteringKey.equals(columnNames.get(i)))
						res += "True,";
					else
						res += "False,";
					res += "False,"; // index
					res += colNameMin.get(columnNames.get(i)) + ",";
					res += colNameMax.get(columnNames.get(i));
					writeToCsv(res);

					File targetDir = new File("src/main/resources/data/");
//				 File targetFile=new File(targetDir, tableName+"_0.page");
					File targetFileTable = new File(targetDir, tableName + ".table");
//		         FileOutputStream fileOut = new FileOutputStream(targetFile);
//		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
//		         out.writeObject(p);
//		         out.close();
//		         fileOut.close();

					FileOutputStream fileOut2 = new FileOutputStream(targetFileTable);
					ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
					out2.writeObject(t);
					out2.close();
					fileOut2.close();

				}
			}
			System.out.println("Table Created!");
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public static boolean checkTableDuplicate(String tableName) { // check table exists
		String row = "";
		BufferedReader csvReader;
		try {
			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
//			
				if (data[0].equalsIgnoreCase(tableName))
					return true;

			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static Tuple checkParamters(String tableName, Hashtable htbl, Table t) throws ParseException {
		Tuple tuple = new Tuple();
		try {
			String row = "";
			String CK = "";
			Enumeration<String> keys = htbl.keys();
			ArrayList columnNames = new ArrayList<>();

			while (keys.hasMoreElements()) {
				String tmp = keys.nextElement().toString();
				columnNames.add(tmp);
			}
			for (int i = 0; i < columnNames.size(); i++) {
				if (!(t.columns.contains(columnNames.get(i)))) {
					return null;
				}
			}
			int[] verify = new int[columnNames.size()];

			BufferedReader csvReader;

			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");

				if (data[0].equalsIgnoreCase(tableName) && columnNames.contains(data[1])) {
					// System.out.println(data[3] == "True");
					if (data[3].equalsIgnoreCase("True")) {
						CK = data[1];
						// System.out.println(res);
					}
					Object tmp = htbl.get(data[1]);
					if (data[2].equalsIgnoreCase(tmp.getClass().getName())) {

						String y = tmp + "";
						boolean flag = false;

						switch (data[2]) {
						case "java.lang.Integer":
							if (Integer.parseInt(data[5]) <= (int) tmp && Integer.parseInt(data[6]) >= (int) tmp) {
								flag = true;
							}
							break;

						case "java.lang.String":
							if ((data[5]).compareToIgnoreCase((String) tmp) <= 0
									&& (data[6]).compareToIgnoreCase((String) tmp) >= 0
									&& (data[5]).length() <=((String) tmp).length()
									&& (data[6]).length() >=((String) tmp).length()
									) {
								flag = true;
							}
							break;

						case "java.lang.Double":
							if (Double.parseDouble(data[5]) <= (double) tmp
									&& Double.parseDouble(data[6]) >= (double) tmp) {
								flag = true;

							}
							break;
//			    				
//			    				String sDate1="31/12/1998";
//			    			    Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);
//			    			    System.out.println(date1);

						case "java.util.Date":
							// System.out.println("hena");
							Date indate = (Date) tmp;
							Date datemax = new SimpleDateFormat("yy-MM-dd").parse(data[6]);
							Date datemin = new SimpleDateFormat("yy-MM-dd").parse(data[5]);
//			    				System.out.println(datemin);
//			    				System.out.println(datemax);
//			    				System.out.println(indate);
//			    				System.out.println(indate.compareTo(datemin)>=0);
							if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
								flag = true;
								// System.out.println("da5al");

							}
							break;
						}

						// System.out.println(flag);
						if (flag)
						// if (y.compareTo(data[5])>=0 && y.compareTo(data[6])<=0)
						// System.out.println((String) columnNames.get(i));
						// System.out.println(data[1]);
						// System.out.println(y);
						{
							for (int i = 0; i < columnNames.size(); i++) {
								if (((String) columnNames.get(i)).equalsIgnoreCase(data[1])) {

									verify[i] = 1;
//			    						System.out.println(verify[i]);
//			    						System.out.println(((String) columnNames.get(i)).equalsIgnoreCase(data[1]));
								}
							}
						}
					}

				} else {
					if (data[0].equalsIgnoreCase(tableName)) {
						htbl.put(data[1], "null");
						columnNames.add(data[1]);
					}
				}
				// System.out.println(verify[1]);
//					 System.out.println((tmp.getClass().getName()));
			}
			for (int i = 0; i < columnNames.size(); i++) {
				if (!t.columns.contains(columnNames.get(i))
						&& !(((String) columnNames.get(i)).equalsIgnoreCase((String) t.columns.get(0))))

					verify[i] = 1;
			}

			csvReader.close();

			for (int i = 0; i < verify.length; i++) {
				if (verify[i] == 0)
					return null;
//					System.out.println(verify[i]);
//					System.out.println(c);
//					System.out.println(nColumns);
			}

			for (int i = 0; i < t.columns.size(); i++) {
				tuple.add(htbl.get(t.columns.get(i)));
			}
			return tuple;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		// return res;

	}

	public double round3(double x) {
		x = x * 1000;
		x = Math.round(x);
		x = x / 1000;
		return x;
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// check loop over el existing table isa bokra
		int dimensions = columnNames.length;
//		Vector columns= new Vector();
		Vector metadata = new Vector();
		BufferedReader csvReader;
		String row = "";
		Vector tableColumns = new Vector();
		Index gridIndex = new Index(tableName);
		Arrays.sort(columnNames);

		for (int i = 0; i < columnNames.length; i++) {
			columnNames[i] = columnNames[i].toLowerCase();
		}

		try {
			Table t = deserialize(tableName);

			for (int i = 0; i < t.indices.size(); i++) {
				String[] tmpIndex = (String[]) t.indices.get(i);
				Arrays.sort(tmpIndex);
				if (Arrays.equals(columnNames, tmpIndex))
					throw new DBAppException();
			}

			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equalsIgnoreCase(tableName)) {
					if (data[3].equalsIgnoreCase("true")) {
						metadata.add(0, data);
						tableColumns.add(0, data[1]);

					} else {
						metadata.add(data);
						tableColumns.add(data[1]);
					}
				}
			}
			for (int i = 0; i < dimensions; i++) {
				if (!(tableColumns.contains(columnNames[i]))) {
					throw new DBAppException(); // column doesn't exist
				} else {
					if (tableColumns.indexOf(columnNames[i]) == 0) { // CK
						gridIndex.columnNames.add(0, columnNames[i]);
					} else {
						gridIndex.columnNames.add(columnNames[i]);

					}

					// System.out.println(columnNames.toString());
//					for(int g = 0; g<gridIndex.columnNames.size(); g++) {
//						System.out.println(gridIndex.columnNames.get(g).toString());
//					}

//					columns.add(new Vector());

				}

			}

			for (int i = 0; i < dimensions; i++) {
				Vector v = new Vector();

				String[] data = (String[]) metadata.get(tableColumns.indexOf(gridIndex.columnNames.get(i)));
				System.out.println(data.toString());
				// String type = data[2];

				// TESTING
//           	for(int g = 0; g<data.length; g++) {
//				System.out.println(g+""+data[g].toString());
//			}
//           	System.out.println("DATA OF 2:" + data[2]);
				System.out.println("data[2]" + data[2]);
				switch (data[2]) {
				case "java.lang.Integer":
					v.add("java.lang.Integer");
					int min = Integer.parseInt(data[5]);
					int max = Integer.parseInt(data[6]);
					int totalInBucket = ((max - min) + 1) / 10; // number of elements in bucket
					if (totalInBucket == 0) {
						for (int j = min; j <= max; j++) {
							Vector tmp = new Vector();
							tmp.add(j);
							tmp.add(j);
							v.add(tmp);
						}
						gridIndex.add(v);
//						t.indices.add(gridIndex);
					} else {
						int current = min;
						for (int j = 0; j < 10; j++) { // for each bucket
							int increment = totalInBucket - 1;
//						int increment = totalInBucket;
							Vector tmp = new Vector();
							tmp.add(current);
							tmp.add(current + increment);
							current = current + increment + 1;
							v.add(tmp);
						}
						if (((max - min) + 1) % 10 != 0) { // difference not divisible by 10
							Vector tmp = new Vector();
							tmp.add(current);
							tmp.add(max);
							v.add(tmp);
						}
						gridIndex.add(v);
//				
					}
					break;

				case "java.lang.String":

					v.add("java.lang.String");
					int minString = asciiCode(data[5]);
					int maxString = asciiCode(data[6]);
					int totalInBucketString = ((maxString - minString) + 1) / 10; // number of elements in bucket
					if (totalInBucketString == 0) {
						for (int j = minString; j <= maxString; j++) {
							Vector tmp = new Vector();
							tmp.add(j);
							tmp.add(j);
							v.add(tmp);
						}
						gridIndex.add(v);
//						t.indices.add(gridIndex);
					} else {
						int current = minString;
						for (int j = 0; j < 10; j++) { // for each bucket
							int increment = totalInBucketString - 1;
//						int increment = totalInBucketString;
							Vector tmp = new Vector();
							tmp.add(current);
							tmp.add(current + increment);
							current = current + increment + 1;
							v.add(tmp);
						}
						if (((maxString - minString) + 1) % 10 != 0) { // difference not divisible by 10
							Vector tmp = new Vector();
							tmp.add(current);
							tmp.add(maxString);
							v.add(tmp);
						}
						gridIndex.add(v);
//				
					}
					break;

				case "java.lang.Double": {
					v.add("java.lang.Double");
					double minDouble = Double.parseDouble(data[5]);
					double maxDouble = Double.parseDouble(data[6]);
					double totalInBucketDouble = ((maxDouble - minDouble) + 0.001) / 10; // number of elements in bucket

					double currentDouble = minDouble;
					for (int j = 0; j < 10; j++) { // for each bucket
						double increment = totalInBucketDouble - 0.01;
//						double increment = totalInBucketDouble;
						Vector tmp = new Vector();

						tmp.add(round3(currentDouble));
						tmp.add(round3(currentDouble + increment));
						currentDouble = round3(currentDouble + increment + 0.001);
						v.add(tmp);
					}
					if (((maxDouble - minDouble) + 1) % 10 != 0) { // difference not divisible by 10
						Vector tmp = new Vector();
						tmp.add(round3(currentDouble));
						tmp.add(round3(maxDouble));
						v.add(tmp);
					}
					gridIndex.add(v);
					break;
				}
//	    				String sDate1="31/12/1998";
//	    			    Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);
//	    			    System.out.println(date1);

				case "java.util.Date":
					// System.out.println("data[2] gowa el date" + data[2]);
					try {
						Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse((String) data[5]);
						Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse((String) data[6]);
						int diffInDays = (int) ((maxDate.getTime() - minDate.getTime()) / (1000 * 60 * 60 * 24));
//						System.out.println(diffInDays);
						v.add("java.util.Date");
						int totalInBucketDate = ((diffInDays) + 1) / 10; // number of elements in bucket
//						if (totalInBucketDate==0) { //condition if the domain is less than 10 days
//							for(int j=min;j<=max;j++) {
//								Vector tmp = new Vector();
//								tmp.add(j);
//								tmp.add(j);
//								v.add(tmp);
//							}
//							gridIndex.add(v);
////							t.indices.add(gridIndex);
//						}
//						else {
						Date current = minDate;
						for (int j = 0; j < 10; j++) { // for each bucket
							int increment = totalInBucketDate - 1; // check
							Vector tmp = new Vector();
							tmp.add(current);
//							String dt = "2008-01-01";  // Start date
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							Calendar c = Calendar.getInstance();
							c.setTime(current);
							c.add(Calendar.DATE, increment); // number of days to add
							current = c.getTime(); // dt is now the new date

							tmp.add(current);
							c.setTime(current);
							c.add(Calendar.DATE, 1);

							current = c.getTime();
							v.add(tmp);
						}
						if (((diffInDays) + 1) % 10 != 0) { // difference not divisible by 10
							Vector tmp = new Vector();
							tmp.add(current);
							tmp.add(maxDate);
							v.add(tmp);
						}
						gridIndex.add(v);
//					
//						}
//CODE TO ADD DAYS						
//						String dt = "2008-01-01";  // Start date
//						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//						Calendar c = Calendar.getInstance();
//						c.setTime(sdf.parse(dt));
//						c.add(Calendar.DATE, 36);  // number of days to add
//						dt = sdf.format(c.getTime());  // dt is now the new date
//						System.out.println(dt);
////////////	

					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					break;
				}

				// END

			}
			
			Vector tmpC = new Vector();
			for(String s:columnNames) {
				tmpC.add(s);
			}
			
			updateCSV(tableName,tmpC);
			t.indices.add(columnNames);
			serialize(t, tableName);
			serialize(gridIndex, "Index" + (t.indices.size() - 1) + tableName + ".index");
			for (int k = 0; k < t.nPages; k++) {

				page p = deserialize(k, tableName);

				try {
					for (int l = 0; l < p.size(); l++) {
						insertIntoIndex((Tuple) p.get(l), k + "", tableName, true);
					}

					for (int l = 0; l < (int) t.NOverflows.get(k); l++) {
						page OF = deserializeOverflow(k + "." + l, tableName);
						for (int i = 0; i < OF.size(); i++) {
							insertIntoIndex((Tuple) OF.get(i), k + "." + l, tableName, true);
						}
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

//			serialize(gridIndex,"Index"+(t.indices.size()-1)+tableName+".index");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException c) {

			c.printStackTrace();
			return;
		}

	}

	public static int findDimension(Index index, String indexFileName, Object value, String tableName, Table t,
			String columnName) throws ParseException { // returns start coordinate 

//		for (int i=0;i<index.columnNames.size();i++) {
//			int tuplePosition =t.columns.indexOf(index.columnNames.get(i));
//			if (tuplePosition >=0) {
		int h = index.columnNames.indexOf(columnName);
		Vector ranges = (Vector) index.get(h);
		String type = (String) ranges.get(0);

		for (int j = 1; j < ranges.size(); j++) {

			switch (type) {
			case "java.lang.Integer":
				if ((int) value >= (int) ((Vector) ranges.get(j)).get(0)
						&& (int) value <= (int) ((Vector) ranges.get(j)).get(1)) {
					return j;
//							j=ranges.size();
				}
				break;

			case "java.lang.String":
				int ascii = asciiCode((String) value);
				if (ascii >= (int) ((Vector) ranges.get(j)).get(0) && ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
					return j;
//							j=ranges.size();
				}
				break;

			case "java.lang.Double":
				if ((double) value >= (double) (((Vector) ranges.get(j)).get(0))
						&& (double) value <= (double) ((Vector) ranges.get(j)).get(1)) {
					return j;
//						j=ranges.size();
				}
				break;
			case "java.util.Date":
				// Date indate = new SimpleDateFormat("yy-MM-dd").parse((String) value);
				Date indate = (Date) value;
				Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
				Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
				if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
					return j;
//							j=ranges.size();
				}
				break;
			}

		}
//			}
		return -1;
	}

	public static String findBucket(Index index, String indexFileName, Tuple tuple, String tableName, Table t)
			throws ParseException {
		Vector positions = new Vector();
		for (int i = 0; i < index.columnNames.size(); i++) {
			int tuplePosition = t.columns.indexOf(index.columnNames.get(i));
			if (tuplePosition >= 0) {
				Vector ranges = (Vector) index.get(i);
				String type = (String) ranges.get(0);

				for (int j = 1; j < ranges.size(); j++) {

					switch (type) {
					case "java.lang.Integer":
						if ((int) tuple.get(tuplePosition) >= (int) ((Vector) ranges.get(j)).get(0)
								&& (int) tuple.get(tuplePosition) <= (int) ((Vector) ranges.get(j)).get(1)) {
							positions.add(j);
							j = ranges.size();
						}
						break;

					case "java.lang.String":
						int ascii = asciiCode((String) tuple.get(tuplePosition));
						if (ascii >= (int) ((Vector) ranges.get(j)).get(0)
								&& ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
							positions.add(j);
							j = ranges.size();
						}
						break;

					case "java.lang.Double":
						if ((double) tuple.get(tuplePosition) >= (double) (((Vector) ranges.get(j)).get(0))
								&& (double) tuple.get(tuplePosition) <= (double) ((Vector) ranges.get(j)).get(1)) {
							positions.add(j);
							j = ranges.size();
						}
						break;
					case "java.util.Date":
						Date indate = new SimpleDateFormat("yy-MM-dd").parse((String) tuple.get(tuplePosition));
						Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
						Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
						if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
							positions.add(j);
							j = ranges.size();
						}
						break;
					}

				}
			}
		}

		String bucketName = indexFileName;
		for (int x = 0; x < positions.size(); x++) {
			bucketName += "_" + positions.get(x);
		}
		return bucketName;
	}

	
	public static Vector findCKBucket(Index index, String indexFileName, String ck, String tableName, Table t) //for update method
			throws ParseException {
		Vector positions = new Vector();
		
		
		int i = index.columnNames.indexOf(t.columns.get(0));
		int ckBucket=-1;	
		Vector result = new Vector();
				Vector ranges = (Vector) index.get(i);
				String type = (String) ranges.get(0);

				for (int j = 1; j < ranges.size(); j++) {

					switch (type) {
					case "java.lang.Integer":
						int tmp = Integer.parseInt(ck);
						if (tmp >= (int) ((Vector) ranges.get(j)).get(0)
								&& tmp<= (int) ((Vector) ranges.get(j)).get(1)) {
							ckBucket = j;
							j = ranges.size();
						
						}
						break;

					case "java.lang.String":
						int ascii = asciiCode((String) ck);
						if (ascii >= (int) ((Vector) ranges.get(j)).get(0)
								&& ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
							ckBucket = j;
							j = ranges.size();
						
						}
						break;

					case "java.lang.Double":
						double tmp2 = Double.parseDouble(ck);
						if (tmp2 >= (double) (((Vector) ranges.get(j)).get(0))
								&& tmp2 <= (double) ((Vector) ranges.get(j)).get(1)) {
							ckBucket = j;
							j = ranges.size();
					
						}
						break;
					case "java.util.Date":
						Date indate = new SimpleDateFormat("yy-MM-dd").parse((String) ck);
						Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
						Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
						if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
							ckBucket = j;
							j = ranges.size();
						
						}
						break;
					}

				}
			
			
				
				
				
				String bucketName = indexFileName+"_"+ckBucket;
				
				result.add(bucketName);

				for (int j=1;j<index.columnNames.size();j++) {
					Vector range = (Vector) index.get(j);
					Vector tmpV = new Vector();
					for(int k=0;k<result.size();k++) {
						for(int z=1;z<range.size();z++)
						{
							tmpV.add(result.get(k) + "_" + z);
						}
					}
					result = tmpV;
				}
			
				switch(type) {
				case "java.lang.Integer":
					int tmp = Integer.parseInt(ck);
					result.add(tmp);				
					break;

				case "java.lang.String":
					result.add(ck);		
					
					break;

				case "java.lang.Double":
					double tmp2 = Double.parseDouble(ck);
					result.add(tmp2);		
				
					
					break;
				case "java.util.Date":
					Date indate = new SimpleDateFormat("yy-MM-dd").parse((String) ck);
					result.add(indate);		
					
					break;
				}
				
		return result;  //first position is the CK with the correct type
	}
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (checkTableDuplicate(tableName) == false) // to check if table exists
		{
			System.out.println("Table Does Not Exist!");
			return;
		}

		try {

			FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".table");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Table t = (Table) in.readObject();
			in.close();
			fileIn.close();

			Tuple tuple = checkParamters(tableName, colNameValue, t);
			if (tuple == null) {
				throw new DBAppException();

			}

			Enumeration<String> keys = colNameValue.keys();
			ArrayList columnNames = new ArrayList<>();

			while (keys.hasMoreElements()) {
				String tmp = keys.nextElement().toString();
				columnNames.add(tmp);
			}

			// we need to update all indices
			int max = -1;
			int maxWithIndex = -1;
			int rightIndex = -1;
			int rightIndexWithCK = -1;
//			for(int i=0;i<t.indices.size();i++) {
//				int score=-1;
//				if(((Vector)t.indices.get(i)).contains(t.columns.get(0))) {
//				for(int j=0;j<((Vector)t.indices.get(i)).size();j++)
//				{
//					if(columnNames.contains(((Vector)t.indices.get(i)).get(j))){
//						score++;
//					}
//				}
//				if(maxWithIndex<score) {
//					maxWithIndex=score;
//					rightIndexWithCK=i;
//				}
//			}
//				else {
//					for(int j=0;j<((Vector)t.indices.get(i)).size();j++)
//					{
//						if(columnNames.contains(((Vector)t.indices.get(i)).get(j))){
//							score++;
//						}
//					}
//					if(maxWithIndex<score) {
//						max=score;
//						rightIndex=i;
//					}
//				}
//			}

			
			 // no usable index found for this insertion
//				for (int i = 0; i < t.columns.size(); i++) {
//					tuple.add(colNameValue.get(t.columns.get(i)));
//				}
			boolean indexExists= false;
			if(t.indices.size()>0) {
				indexExists=true;
			}
			int pageNum=-1;
//				System.out.println(tuple.get(0));
				if (t.nPages == 0) {
//				System.out.println("zero pages");
					page p = new page();
					p.add(tuple);
//				p.size()++;
					t.ranges.add(tuple.get(0));
					t.ranges.add(tuple.get(0));
					t.nPages++;
					t.sizes.add(1);
					t.NOverflows.add(0);
					Vector v = new Vector();
					Vector v2 = new Vector();
					t.overFlowRanges.add(v);
					t.overFlowSizes.add(v2);

					File targetDir = new File("src/main/resources/data/");
					File targetFile = new File(targetDir, tableName + "_0.page");
					FileOutputStream fileOut = new FileOutputStream(targetFile);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(p);
					out.close();
					fileOut.close();
					serialize(t, tableName);
				} else {
					if (t.ranges.contains(tuple.get(0)))
						System.out.println("Duplicate Key!"); // throw exception?
					else {

						t.ranges.add(tuple.get(0));
//					 System.out.println(t.ranges.toString() + "before sorting");
						Collections.sort(t.ranges);
//					 System.out.println(t.ranges.toString() + "after sorting");

						int position = t.ranges.indexOf(tuple.get(0));
//					 System.out.println(position + "position");
						 pageNum = (position - 1) / 2;
//					 System.out.println(pageNum + "pageNum");
						if (position % 2 == 1) {
							t.ranges.remove(position);
//						System.out.println("position%2==1");
							insertTuple(pageNum, tuple, tableName, t, indexExists);

						} else {
							if (position == 0) {
//							 System.out.println("position==0");
								t.ranges.remove(1);
								insertTuple(pageNum, tuple, tableName, t, indexExists);

							} else {
								t.ranges.remove(position - 1);
								insertTuple(pageNum, tuple, tableName, t, indexExists);
							}
						}
					}
					
				}

				
				
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException i) {
			i.printStackTrace();
			return;
		} catch (ClassNotFoundException c) {

			c.printStackTrace();
			return;
		}
//		catch (DBAppException c) {
//
//			System.out.println("Insertion Failed :(");
//			return;
//		}

	}

	public static int asciiCode(String str) {
		int res = 0;
		for (int i = 0; i < str.length(); i++) {
			res += (int) str.charAt(i);
		}
		return res;
	}

	public static void insertIntoIndex(Tuple tuple, String pageNum, String tableName, boolean newIndex)
			throws ClassNotFoundException, IOException, ParseException {
		Table t = deserialize(tableName);

		if (newIndex == true) {

			Index index = (Index) deserializeGeneral("Index" + (t.indices.size() - 1) + tableName + ".index");
			String indexFileName = "Index" + (t.indices.size() - 1) + tableName;

			Vector positions = new Vector();
			for (int i = 0; i < index.columnNames.size(); i++) {
				int tuplePosition = t.columns.indexOf(index.columnNames.get(i));
//				System.out.println(t.columns.toString());
//				System.out.println(index.columnNames);
				// System.out.println(tuplePosition);
				if (tuplePosition >= 0) {
					Vector ranges = (Vector) index.get(i);
					// System.out.println(ranges.toString());
					String type = (String) ranges.get(0);

					for (int j = 1; j < ranges.size(); j++) {
//						System.out.println(type);
						switch (type) {
						case "java.lang.Integer":
							if ((int) tuple.get(tuplePosition) >= (int) ((Vector) ranges.get(j)).get(0)
									&& (int) tuple.get(tuplePosition) <= (int) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;

						case "java.lang.String":
							int ascii = asciiCode((String) tuple.get(tuplePosition));
							if (ascii >= (int) ((Vector) ranges.get(j)).get(0)
									&& ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;

						case "java.lang.Double":
							if ((double) tuple.get(tuplePosition) >= (double) (((Vector) ranges.get(j)).get(0))
									&& (double) tuple.get(tuplePosition) <= (double) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;
						case "java.util.Date":
							// Date indate = new
							// SimpleDateFormat("yy-MM-dd").parse((String)tuple.get(tuplePosition));
							Date indate = (Date) tuple.get(tuplePosition);
							Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
							Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
							if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
								positions.add(j);
								j = ranges.size();
							}
							break;
						}

					}
				}
			}

			String bucketName = indexFileName;
			for (int x = 0; x < positions.size(); x++) {
				bucketName += "_" + positions.get(x);
			}
//					bucketName+=".bucket";  //bucketname

			Tuple tuple2 = new Tuple();
			for (int m = 0; m < index.columnNames.size(); m++) {
				int position = t.columns.indexOf(index.columnNames.get(m));
				tuple2.add(tuple.get(position));
			}
			tuple2.add(pageNum); // tuple in bucket

			if (index.buckets.contains(bucketName)) {
				page b = (page) deserializeGeneral(bucketName + ".bucket");
				if ((int) index.bucketOF.get(bucketName) > 0) { // overflow

					int NOverflows = (int) index.bucketOF.get(bucketName);
					if (((Tuple) b.get(b.size() - 1)).compareTo(tuple2) > 0) { // no overflows and space in bucket
						if (b.size() < maxBuckets) {
//								index.buckets.add(bucketName);
//								index.bucketOF.put(bucketName, 0);
							b.add(tuple2);
							Collections.sort(b);
							serialize(index, indexFileName + ".index");
							serialize(b, bucketName + ".bucket");
						} else { // insert and move into next overflows
							b.add(tuple2);
							Collections.sort(b);
							Tuple removed = (Tuple) b.remove(b.size() - 1);
							for (int i1 = 0; i1 < NOverflows; i1++) {
								page bucketOF = (page) deserializeGeneral(bucketName + "overbucket" + i1);
								// shifting in overflow bucket ba3deen

							}
							serialize(index, indexFileName + ".index");
							serialize(b, bucketName + ".bucket");
						}
					} else {
						for (int i1 = 0; i1 < NOverflows; i1++) {
							// check
						}
					}
				} else {
					if (b.size() < maxBuckets) { // no overflows and space in bucket
//								index.buckets.add(bucketName);
//								index.bucketOF.put(bucketName, 0);
						b.add(tuple2);
						Collections.sort(b);
						serialize(index, indexFileName + ".index");
						serialize(b, bucketName + ".bucket");
					} else { // create an overflow
//								index.buckets.add(bucketName);
						index.bucketOF.put(bucketName, 0);
						b.add(tuple2);
						Collections.sort(b);
						Tuple removed = (Tuple) b.remove(b.size() - 1);
						page b2 = new page();
						b2.add(removed);
						serialize(b2, bucketName + ".overbucket" + (int) index.bucketOF.get(bucketName));
						index.bucketOF.replace(bucketName, (int) index.bucketOF.get(bucketName) + 1);

						serialize(index, indexFileName + ".index");
						serialize(b, bucketName + ".bucket");
					}
				}

			}

			else {
				index.buckets.add(bucketName);
				index.bucketOF.put(bucketName, 0);
				page b = new page();
				b.add(tuple2);
				serialize(index, indexFileName + ".index");
				serialize(b, bucketName + ".bucket");
			}
		}

		if (newIndex == false) {
			pageNum = lastPage;
			for (int number = 0; number < t.indices.size(); number++) {
				Index index = (Index) deserializeGeneral("Index" + number + tableName + ".index");
				String indexFileName = "Index" + number + tableName;
				// System.out.println(index);

				Vector positions = new Vector();
				for (int i = 0; i < index.columnNames.size(); i++) {
					int tuplePosition = t.columns.indexOf(index.columnNames.get(i));
//			System.out.println(t.columns.toString());
//			System.out.println(index.columnNames);
					// System.out.println(tuplePosition);
					if (tuplePosition >= 0) {
						Vector ranges = (Vector) index.get(i);
						// System.out.println(ranges.toString());
						String type = (String) ranges.get(0);

						for (int j = 1; j < ranges.size(); j++) {
//					System.out.println(type);
							switch (type) {
							case "java.lang.Integer":
								if ((int) tuple.get(tuplePosition) >= (int) ((Vector) ranges.get(j)).get(0)
										&& (int) tuple.get(tuplePosition) <= (int) ((Vector) ranges.get(j)).get(1)) {
									positions.add(j);
									j = ranges.size();
								}
								break;

							case "java.lang.String":
								int ascii = asciiCode((String) tuple.get(tuplePosition));
								if (ascii >= (int) ((Vector) ranges.get(j)).get(0)
										&& ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
									positions.add(j);
									j = ranges.size();
								}
								break;

							case "java.lang.Double":
								if ((double) tuple.get(tuplePosition) >= (double) (((Vector) ranges.get(j)).get(0))
										&& (double) tuple.get(tuplePosition) <= (double) ((Vector) ranges.get(j))
												.get(1)) {
									positions.add(j);
									j = ranges.size();
								}
								break;
							case "java.util.Date":
								Date indate = new SimpleDateFormat("yy-MM-dd").parse((String) tuple.get(tuplePosition));
								Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
								Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
								if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
									positions.add(j);
									j = ranges.size();
								}
								break;
							}

						}
					}
				}

				String bucketName = indexFileName;
				for (int x = 0; x < positions.size(); x++) {
					bucketName += "_" + positions.get(x);
				}
//				bucketName+=".bucket";  //bucketname

				Tuple tuple2 = new Tuple();
				for (int m = 0; m < index.columnNames.size(); m++) {
					int position = t.columns.indexOf(index.columnNames.get(m));
					tuple2.add(tuple.get(position));
				}
				tuple2.add(lastPage); // tuple in bucket

				if (index.buckets.contains(bucketName)) {
					page b = (page) deserializeGeneral(bucketName + ".bucket");
					if ((int) index.bucketOF.get(bucketName) > 0) { // overflow

						int NOverflows = (int) index.bucketOF.get(bucketName);
						if (((Tuple) b.get(b.size() - 1)).compareTo(tuple2) > 0) { // no overflows and space in bucket
							if (b.size() < maxBuckets) {
//							index.buckets.add(bucketName);
//							index.bucketOF.put(bucketName, 0);
								b.add(tuple2);
								Collections.sort(b);
								serialize(index, indexFileName + ".index");
								serialize(b, bucketName + ".bucket");
							} else { // insert and move into next overflows
								b.add(tuple2);
								Collections.sort(b);
								Tuple removed = (Tuple) b.remove(b.size() - 1);
								for (int i1 = 0; i1 < NOverflows; i1++) {
									page bucketOF = (page) deserializeGeneral(bucketName + "overbucket" + i1);
									// shifting in overflow bucket ba3deen

								}
								serialize(index, indexFileName + ".index");
								serialize(b, bucketName + ".bucket");
							}
						} else {
							for (int i1 = 0; i1 < NOverflows; i1++) {

							}
						}
					} else {
						if (b.size() < maxBuckets) { // no overflows and space in bucket
//							index.buckets.add(bucketName);
//							index.bucketOF.put(bucketName, 0);
							b.add(tuple2);
							Collections.sort(b);
							serialize(index, indexFileName + ".index");
							serialize(b, bucketName + ".bucket");
						} else { // create an overflow
//							index.buckets.add(bucketName);
							index.bucketOF.put(bucketName, 0);
							b.add(tuple2);
							Collections.sort(b);
							Tuple removed = (Tuple) b.remove(b.size() - 1);
							page b2 = new page();
							b2.add(removed);
							serialize(b2,bucketName + ".overbucket" + (int) index.bucketOF.get(bucketName + ".bucket"));
							index.bucketOF.replace(bucketName + ".bucket",
									(int) index.bucketOF.get(bucketName + ".bucket") + 1);

							serialize(index, indexFileName + ".index");
							serialize(b, bucketName + ".bucket");
						}
					}

				}

				else {
					index.buckets.add(bucketName);
					index.bucketOF.put(bucketName, 0);
					page b = new page();
					b.add(tuple2);
					serialize(index, indexFileName + ".index");
					serialize(b, bucketName + ".bucket");
				}
			}
		}
	}

	public static void deleteFromIndex(Tuple tuple, String pageNum, String tableName)
			throws ClassNotFoundException, IOException, ParseException {
		Table t = deserialize(tableName);

		for (int number = 0; number < t.indices.size(); number++) {
			Index index = (Index) deserializeGeneral("Index" + number + tableName + ".index");
			String indexFileName = "Index" + number + tableName;

			Tuple fixedTuple = new Tuple();

			for (int i = 0; i < index.columnNames.size(); i++) {
				fixedTuple.add(tuple.get(t.columns.indexOf(index.columnNames.get(i))));
			}
			fixedTuple.add(pageNum);
			System.out.println("FIXED TUPLE" + fixedTuple);
			Vector positions = new Vector();
			for (int i = 0; i < index.columnNames.size(); i++) {
				int tuplePosition = t.columns.indexOf(index.columnNames.get(i));
				if (tuplePosition >= 0) {
					Vector ranges = (Vector) index.get(i);
					String type = (String) ranges.get(0);

					for (int j = 1; j < ranges.size(); j++) {

						switch (type) {
						case "java.lang.Integer":
							if ((int) tuple.get(tuplePosition) >= (int) ((Vector) ranges.get(j)).get(0)
									&& (int) tuple.get(tuplePosition) <= (int) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;

						case "java.lang.String":
							int ascii = asciiCode((String) tuple.get(tuplePosition));
							if (ascii >= (int) ((Vector) ranges.get(j)).get(0)
									&& ascii <= (int) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;

						case "java.lang.Double":
							if ((double) tuple.get(tuplePosition) >= (double) (((Vector) ranges.get(j)).get(0))
									&& (double) tuple.get(tuplePosition) <= (double) ((Vector) ranges.get(j)).get(1)) {
								positions.add(j);
								j = ranges.size();
							}
							break;
						case "java.util.Date":
							// Date indate = new SimpleDateFormat("yy-MM-dd").parse((String)
							// tuple.get(tuplePosition));
							Date indate = (Date) tuple.get(tuplePosition);
							Date datemax = (Date) ((Vector) ranges.get(j)).get(1);
							Date datemin = (Date) ((Vector) ranges.get(j)).get(0);
							if (indate.compareTo(datemin) >= 0 && indate.compareTo(datemax) <= 0) {
								positions.add(j);
								j = ranges.size();
							}
							break;
						}

					}
				}
			}

			String bucketName = indexFileName;
			for (int x = 0; x < positions.size(); x++) {
				bucketName += "_" + positions.get(x); // CHECK this does not consider if it is in the overflow
			}

			page b = (page) deserializeGeneral(bucketName + ".bucket");
			int positionRes = Collections.binarySearch(b, fixedTuple);

			if (positionRes < 0) {
				int NBuckets = (int) index.bucketOF.get(bucketName);
				// search in overflows //check
			} else {
				boolean flag = false;
				while (!flag) {
					
					if (b.size()>0 && b.get(positionRes).equals(fixedTuple)) {
						b.remove(positionRes);
//						positionRes++;
					} else {
						if(b.size()==0) {
							flag = true;
						}
						else if (((Tuple) b.get(positionRes)).get(0) != fixedTuple.get(0)) {
							flag = true;
						} else {
							positionRes++;
							if (positionRes==b.size()) {
								flag = true;
							}
						}
					}

				}
				serialize(b, bucketName + ".bucket");
			}
//		System.out.println(fixedTuple.toString());
		}
	}

//	public void updateIndex(Index index, String indexFileName, Tuple oldTuple,Tuple newTuple, String oldPos,String newPos,String tableName) throws ClassNotFoundException, IOException, ParseException {
//		deleteFromIndex(oldTuple, oldPos, tableName);
//		insertIntoIndex(newTuple, newPos, tableName);
//	}

	public static void insertTuple(int pageNum, Tuple tuple, String tableName, Table t, boolean fromIndex)
			throws ParseException {

		try {
			FileInputStream fileIn = new FileInputStream(
					"src/main/resources/data/" + tableName + "_" + pageNum + ".page");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page p = (page) in.readObject();
			in.close();
			fileIn.close();
			boolean flag = false;

			if (p.containsKey(tuple)) {
				System.out.println("Duplicate Key!");
				return;
			} else {
				if (p.size() < maxRows) {
					if ((int) t.NOverflows.get(pageNum) > 0) {
						if (tuple.compareTo((Tuple) p.get(p.size() - 1)) > 0) {
							Vector res = CKSearchOverflow(p, pageNum + "", tableName, tuple, t);
							String[] splitt = ((String) res.get(2)).split("\\.");
							int splittRes = Integer.parseInt(splitt[1]);
							insertTupleOverFlow((String) res.get(2), splittRes, tuple, tableName, t, p, pageNum,
									fromIndex);
						} else {
							p.add(tuple);
							lastPage = pageNum+"";
							if (fromIndex) {
								insertIntoIndex(tuple, pageNum + "", tableName, false);
							}
							Collections.sort(p);
//			        	 System.out.println(p.v.toString());
//			        	System.out.println("pageNum*2 " + pageNum*2);
//			        	System.out.println(t.ranges.toString());
//			        	System.out.println("((Tuple)p.v.get(0)).v.get(0) " +((Tuple)p.v.get(0)).v.get(0) );
							t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
							t.ranges.set((pageNum * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
							t.sizes.set(pageNum, p.size());
							serialize(p, pageNum, tableName);
							serialize(t, tableName);
						}
					} else {
						p.add(tuple);
						lastPage= pageNum+"";
						if (fromIndex) {
							insertIntoIndex(tuple, pageNum + "", tableName, false);
						}
						Collections.sort(p);
//	        	 System.out.println(p.v.toString());
//	        	System.out.println("pageNum*2 " + pageNum*2);
//	        	System.out.println(t.ranges.toString());
//	        	System.out.println("((Tuple)p.v.get(0)).v.get(0) " +((Tuple)p.v.get(0)).v.get(0) );
						t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
						t.ranges.set((pageNum * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
						t.sizes.set(pageNum, p.size());
						serialize(p, pageNum, tableName);
						serialize(t, tableName);
					}
				} else {
					// no empty space in current page -> check for next page
//	        	 System.out.println("pageNum:" +pageNum + "   " +(pageNum+1));
//	        	 System.out.println(t.sizes.size());
//	        	 System.out.println(t.sizes.size()==(pageNum+1));
					if (t.sizes.size() == (pageNum + 1)) // last page
					{
						if ((int) t.NOverflows.get(pageNum) > 0) {
							if (tuple.compareTo((Tuple) p.get(p.size() - 1)) > 0) {
								Vector res = CKSearchOverflow(p, pageNum + "", tableName, tuple, t);
								String[] splitt = ((String) res.get(2)).split("\\.");
								int splittRes = Integer.parseInt(splitt[1]);
								insertTupleOverFlow((String) res.get(2), splittRes, tuple, tableName, t, p, pageNum,
										fromIndex);
							} else {
								p.add(tuple);
								lastPage= pageNum+"";
								if (fromIndex) { // check
									insertIntoIndex(tuple, pageNum + "", tableName, false);
								}
								Collections.sort(p);
//				        	 System.out.println(p.v.toString());
//				        	System.out.println("pageNum*2 " + pageNum*2);
//				        	System.out.println(t.ranges.toString());
//				        	System.out.println("((Tuple)p.v.get(0)).v.get(0) " +((Tuple)p.v.get(0)).v.get(0) );
								t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
								t.ranges.set((pageNum * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
								t.sizes.set(pageNum, p.size());
								serialize(p, pageNum, tableName);
								serialize(t, tableName);
							}
						} else {
							// fix here
//						System.out.println("No space in current page and making a new page");
							page p2 = new page();
							p.add(tuple);
							lastPage= pageNum+"";
							if (fromIndex) {
								insertIntoIndex(tuple, pageNum + "", tableName, false);
							}
							Collections.sort(p);
							Tuple t2 = (Tuple) p.remove(p.size() - 1);
							p2.add(t2);
							if (fromIndex) {
								deleteFromIndex(t2, pageNum + "", tableName);
								lastPage= (pageNum+1)+"";
								insertIntoIndex(t2, (pageNum + 1) + "", tableName, false);
							}
//						p2.size()++;
							t.nPages++;
							t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
							t.ranges.set((pageNum * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
							t.ranges.add(t2.get(0));
							t.ranges.add(t2.get(0));
							t.sizes.add(1);
							t.NOverflows.add(0);
							Vector v = new Vector();
							Vector v2 = new Vector();
							t.overFlowRanges.add(v);
							t.overFlowSizes.add(v2);

							serialize(t, tableName);
							serialize(p, pageNum, tableName);
							serialize(p2, pageNum + 1, tableName);
						}
					} else if ((int) t.sizes.get(pageNum + 1) < maxRows) { // next page has space
						page p2 = deserialize(pageNum + 1, tableName);
						p.add(tuple); // stopped here

						Collections.sort(p);

						Tuple t2 = (Tuple) p.remove(p.size() - 1);
						p2.add(0, t2);
						if (fromIndex) {
							lastPage= pageNum+"";
							insertIntoIndex(tuple, pageNum + "", tableName, false);
						}
						if (fromIndex) {
							deleteFromIndex(t2, pageNum + "", tableName);
							lastPage= (pageNum+1)+"";
							insertIntoIndex(t2, (pageNum + 1) + "", tableName, false);
						}
//						p2.size()++;

						t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
						t.ranges.set((pageNum * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
						t.ranges.set((pageNum + 1) * 2, ((Tuple) p2.get(0)).get(0));
//						System.out.println((pageNum + 1) * 2);
//						System.out.println(((Tuple) p2.get(0)).get(0));
						t.ranges.set(((pageNum + 1) * 2) + 1, ((Tuple) p2.get(p2.size() - 1)).get(0));

						System.out.println(((Tuple) p.get(0)).get(0));
						System.out.println(((Tuple) p.get(p.size() - 1)).get(0));
						System.out.println(((Tuple) p2.get(0)).get(0));
						System.out.println(((Tuple) p2.get(p2.size() - 1)).get(0));

						t.sizes.set(pageNum + 1, p2.size());

						serialize(t, tableName);
						serialize(p, pageNum, tableName);
						serialize(p2, pageNum + 1, tableName);
					} else { // new overflow
						if (((Vector) t.overFlowSizes.get(pageNum)).size() == 0) {
							///////
							page overflow = new page();
							p.add(tuple);
							Collections.sort(p);
							Tuple t2 = (Tuple) p.remove(p.size() - 1);
							overflow.add(0, t2);
							lastPage= pageNum+"";
							if (fromIndex) {
								insertIntoIndex(tuple, pageNum + "", tableName, false);
							}
							if (fromIndex) {
								deleteFromIndex(t2, pageNum + "", tableName);
								lastPage= (pageNum) + ".0";
								insertIntoIndex(t2, (pageNum) + ".0", tableName, false);
							}
//							overflow.size()++;
							((Vector) t.overFlowSizes.get(pageNum)).add(1);
							((Vector) t.overFlowRanges.get(pageNum)).add(t2.get(0));
							((Vector) t.overFlowRanges.get(pageNum)).add(t2.get(0));
							t.ranges.set(pageNum * 2, ((Tuple) p.get(0)).get(0));
							t.ranges.set((pageNum * 2) + 1, ((Tuple) overflow.get(overflow.size() - 1)).get(0));
							t.NOverflows.set(pageNum, (int) t.NOverflows.get(pageNum) + 1);
							serialize(t, tableName);
							serialize(p, pageNum, tableName);
							serializeOverflow(overflow, pageNum + ".0", tableName);
						}

						else { // current overflows

							if (((Vector) t.overFlowRanges.get(pageNum)).contains(tuple.get(0)))
								System.out.println("Duplicate Key!");
							else {

								((Vector) t.overFlowRanges.get(pageNum)).add(tuple.get(0));
//						 System.out.println(t.ranges.toString() + "before sorting");
								Collections.sort(((Vector) t.overFlowRanges.get(pageNum)));
//						 System.out.println(t.ranges.toString() + "after sorting");

								int position = ((Vector) t.overFlowRanges.get(pageNum)).indexOf(tuple.get(0));
//						 System.out.println(position + "position");
								int pageNumO = (position - 1) / 2;
								String overFlowPage = pageNum + "." + pageNumO;
//						 System.out.println(pageNum + "pageNum");
								if (position % 2 == 1) {
									((Vector) t.overFlowRanges.get(pageNum)).remove(position);
//							System.out.println("position%2==1");
									insertTupleOverFlow(overFlowPage, pageNumO, tuple, tableName, t, p, pageNum,
											fromIndex);
								} else {
									if (position == 0) {
//								 System.out.println("position==0");
										((Vector) t.overFlowRanges.get(pageNum)).remove(1);
										insertTupleOverFlow(overFlowPage, pageNumO, tuple, tableName, t, p, pageNum,
												fromIndex);

									} else {
										((Vector) t.overFlowRanges.get(pageNum)).remove(position - 1);
										insertTupleOverFlow(overFlowPage, pageNumO, tuple, tableName, t, p, pageNum,
												fromIndex);
									}
								}
							}

						}
					}
				}
			}
		}

		catch (IOException i) {
			i.printStackTrace();
			return;
		} catch (ClassNotFoundException c) {

			c.printStackTrace();
			return;
		}
	}

	public static Vector CKSearchOverflow(page p, String pageNum, String tableName, Tuple tuple, Table t)
			throws ClassNotFoundException, IOException {
		Vector res = new Vector();
		if (((Tuple) p.get(p.size() - 1)).compareTo(tuple) >= 0) {
			int positionRes = Collections.binarySearch(p, tuple);
			if (positionRes < 0) {
				System.out.println("Not Found");
				return null;
			} else {
				res.add(positionRes);
				res.add(p);
				res.add(pageNum);
				res.add(tableName);
				return res;
			}
		} else {
			int position;
			int pageNumInt = Integer.parseInt(pageNum);
			Vector v = ((Vector) t.overFlowRanges.get(pageNumInt));
			if (v.contains(tuple.get(0))) {
				position = (v.indexOf(tuple.get(0)));
				page p2 = deserializeOverflow(pageNum + "." + position / 2, tableName);

				if (position % 2 == 1) {
					res.add(p2.size() - 1);
					res.add(p2);
					res.add(pageNum + "." + position / 2);
					res.add(tableName);
				} else {
					res.add(0);
					res.add(p2);
					res.add(pageNum + "." + position / 2);
					res.add(tableName);
				}
				return res;
			}

			else {
				v.add(tuple.get(0));
//		 System.out.println(t.ranges.toString() + "before sorting");
				Collections.sort(v);
//		 System.out.println(t.ranges.toString() + "after sorting");

				position = v.indexOf(tuple.get(0));
//		 System.out.println(position + "position");
				int overFlowP = (position - 1) / 2;
//		 System.out.println(pageNum + "pageNum");
				if (position % 2 == 1) {
					v.remove(position);
					page p2 = deserializeOverflow(pageNum + "." + overFlowP, tableName);
					int positionRes = Collections.binarySearch(p2, tuple);
					res.add(positionRes);
					res.add(p2);
					res.add(pageNum + "." + overFlowP);
					res.add(tableName);
					return res;

				} else {
					if (position == 0) {
//				 System.out.println("position==0");
						v.remove(1);
						page p2 = deserializeOverflow(pageNum + "." + overFlowP, tableName);
						int positionRes = Collections.binarySearch(p2, tuple);
						res.add(positionRes);
						res.add(p2);
						res.add(pageNum + "." + overFlowP);
						res.add(tableName);
						return res;
					} else {
						v.remove(position - 1);
						page p2 = deserializeOverflow(pageNum + "." + overFlowP, tableName);
						int positionRes = Collections.binarySearch(p2, tuple);
						res.add(positionRes);
						res.add(p2);
						res.add(pageNum + "." + overFlowP);
						res.add(tableName);
						return res;
					}
				}
			}

		}
//		
	}

	public static Vector CKSearch(Table t, String tableName, Hashtable htbl) {
		if (checkTableDuplicate(tableName) == false) // to check if table exists
		{
			System.out.println("Table Does Not Exist!");
			return null;
		}

		try {

			Tuple tuple = checkParamters(tableName, htbl, t);
			Vector res = new Vector();
			if (tuple == null) {
				System.out.println("Invalid Input!");
				return null;
			}

//				for (int i = 0; i < t.columns.size(); i++) {
//					tuple.add(colNameValue.get(t.columns.get(i)));
//				}
//				System.out.println(tuple.get(0));
			if (t.nPages == 0) {
//				System.out.println("zero pages");
				return null;
			} else {
				int position;
				if (t.ranges.contains(tuple.get(0))) {
					position = (t.ranges.indexOf(tuple.get(0)));
					page p = deserialize(position / 2, tableName);

					if (position % 2 == 1) {
						res = CKSearchOverflow(p, position / 2 + "", tableName, tuple, t);
					} else {
						res.add(0);
						res.add(p);
						res.add((position / 2) + "");
						res.add(tableName);
					}
					return res;
				} else {

					t.ranges.add(tuple.get(0));
//					 System.out.println(t.ranges.toString() + "before sorting");
					Collections.sort(t.ranges);
//					 System.out.println(t.ranges.toString() + "after sorting");

					position = t.ranges.indexOf(tuple.get(0));
//					 System.out.println(position + "position");
					int pageNum = (position - 1) / 2;
//					 System.out.println(pageNum + "pageNum");
					if (position % 2 == 1) {
						t.ranges.remove(position);
						page p = deserialize(pageNum, tableName);
//						if (p.contains(tuple)) {

//						}
						res = CKSearchOverflow(p, pageNum + "", tableName, tuple, t);
						return res;

					} else {
						if (position == 0) {
//							 System.out.println("position==0");
							t.ranges.remove(1);
							page p = deserialize(pageNum, tableName);
							res = CKSearchOverflow(p, pageNum + "", tableName, tuple, t);
							return res;
						} else {
							t.ranges.remove(position - 1);
							page p = deserialize(pageNum, tableName);
							res = CKSearchOverflow(p, pageNum + "", tableName, tuple, t);
							return res;
						}
					}
				}
			}

		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {

			c.printStackTrace();
			return null;
		}
		return null;
	}

	public static boolean CheckNonKeys(Table table, Tuple t, Hashtable htbl) { // checks if non key columns in the input
																				// match the table
		for (int i = 0; i < table.columns.size(); i++) {
			if (htbl.containsKey(table.columns.get(i)) && htbl.get(table.columns.get(i)) != "null") {
				Object x = htbl.get(table.columns.get(i));

				Object y = t.get(i);
//System.out.println("Y"+y.toString());
				try {
					int res = 2;
					String type = x.getClass().getName();

					switch (type) {
					case "java.lang.Integer":
						if ((int) x < (int) y) {
							res = -1;
						} else {
							if ((int) x > (int) y)
								res = 1;
							else
								res = 0;
						}

						break;

					case "java.lang.String":
						res = (((String) x).compareToIgnoreCase((String) y)); {

					}
						break;

					case "java.lang.Double":
						if ((double) x < (double) y) {
							res = -1;
						} else if ((double) x > (double) y) {
							res = 1;
						} else
							res = 0;
						break;

					case "java.util.Date":
						Date indate;
						indate = (Date) x;

						Date date2 = (Date) y;

						res = indate.compareTo(date2);

						break;
					}
					if (res != 0) {
						return false;
					}

				}

				catch (Exception e) {
					return false;
				}

			}

		}
		return true;
	}

	public static void insertTupleOverFlow(String pageNum, int OFNum, Tuple tuple, String tableName, Table t, page p,
			int bigPage, boolean fromIndex) throws ClassNotFoundException, IOException, ParseException {

		page overFlowP = deserializeOverflow(pageNum, tableName);
//		System.out.println("overFlow page num " + pageNum);
		if (((Tuple) p.get(p.size() - 1)).compareTo(tuple) > 0) { // last tuple in overflow page bigger than inserted
																	// tuple
//			System.out.println(((Tuple) p.get(p.size() - 1)).compareTo(tuple) > 0);
//			System.out.println("Tuple current:" + tuple.toString());
//			System.out.println("Tuple in method:" + p.get(p.size() - 1).toString());
//			return;
			p.add(tuple);
			Collections.sort(p);
			lastPage = pageNum+"";
			if (fromIndex) {
				insertIntoIndex(tuple, pageNum, tableName, false);
			}

			tuple = (Tuple) p.remove(p.size() - 1);

			serialize(p, bigPage, tableName);
			insertTupleOverFlow(pageNum, 0, tuple, tableName, t, p, bigPage, fromIndex);
			return;
		} else if (overFlowP.containsKey(tuple)) {
			System.out.println("Duplicate Key!");
			return;
		} else {

			if (overFlowP.size() < maxRows) { // insert in this overflow
				overFlowP.add(tuple);
//				overFlowP.size()++;
				lastPage = pageNum + "." + OFNum;
				if (fromIndex) {
					insertIntoIndex(tuple, pageNum + "." + OFNum, tableName, false);
				}
				Collections.sort(overFlowP);
//		        	 System.out.println(p.v.toString());
//		        	System.out.println("pageNum*2 " + pageNum*2);
//		        	System.out.println(t.ranges.toString());
//		        	System.out.println("((Tuple)p.v.get(0)).v.get(0) " +((Tuple)p.v.get(0)).v.get(0) );
				int x = (int) ((Vector) t.overFlowSizes.get(bigPage)).get(OFNum) + 1;
				((Vector) t.overFlowSizes.get(bigPage)).set(OFNum, x);
				((Vector) t.overFlowRanges.get(bigPage)).set(OFNum * 2, ((Tuple) overFlowP.get(0)).get(0));
				((Vector) t.overFlowRanges.get(bigPage)).set((OFNum * 2) + 1,
						((Tuple) overFlowP.get(overFlowP.size() - 1)).get(0));
				t.ranges.set((bigPage * 2) + 1, ((Vector) t.overFlowRanges.get(bigPage))
						.get(((Vector) t.overFlowRanges.get(bigPage)).size() - 1));
//				((Vector) t.overFlowRanges.get(bigPage)).set(OFNum, overFlowP.size());  // check to fix
				serialize(p, bigPage, tableName);
				serialize(t, tableName);
				serializeOverflow(overFlowP, pageNum, tableName);
			} else { // shifting between overflows

				int distance = 0;
//				int direction = -1; // left 0, right 1
				int direction = 1; // direction is always right
				boolean flag = false;
				if (overFlowP.size() == maxRows) {

					for (int i = 0; i < ((Vector) t.overFlowRanges.get(bigPage)).size(); i++) {
						if ((int) ((Vector) t.overFlowRanges.get(bigPage)).get(i) < maxRows)
							if (i < OFNum) {
								direction = 0;
								distance = OFNum - i;
							} else {
								if (direction == 0) {
									if (i - OFNum < distance) {
										direction = 1;
										break;
									}
								} else {
									direction = 1;
									break;
								}
							}

					}
					if (direction == -1)
						direction = 1;

					if (direction == 1) { // shift right loop
						while (!flag) {
							overFlowP.add(tuple);
							lastPage = pageNum + "." + OFNum;
							if (fromIndex) {
								insertIntoIndex(tuple, pageNum + "." + OFNum, tableName, false);
							}
							Collections.sort(overFlowP);
//							System.out.println("overFlowP: " + overFlowP.toString());
							String OFString = bigPage + "." + OFNum;
							if (overFlowP.size() > maxRows) {
								tuple = (Tuple) overFlowP.remove((overFlowP.size() - 1));
								if (fromIndex) {
									deleteFromIndex(tuple, pageNum + "." + OFNum, tableName);
								}
								((Vector) t.overFlowRanges.get(bigPage)).set(OFNum * 2,
										((Tuple) overFlowP.get(0)).get(0));
								((Vector) t.overFlowRanges.get(bigPage)).set(((OFNum * 2) + 1),
										((Tuple) overFlowP.get(overFlowP.size() - 1)).get(0));
								OFString = bigPage + "." + OFNum;
								serialize(t, tableName);
								serializeOverflow(overFlowP, OFString, tableName);
								OFNum++;
								OFString = bigPage + "." + OFNum;
								if (OFNum == ((Vector) t.overFlowSizes.get(bigPage)).size()) { // last overflow page
									page newPage = new page();
									newPage.add(tuple);
									lastPage = OFString;
									if (fromIndex) {
										insertIntoIndex(tuple, OFString, tableName, false);
									}
//									newPage.size()++;
									((Vector) t.overFlowRanges.get(bigPage)).add(tuple.get(0));
									((Vector) t.overFlowRanges.get(bigPage)).add(tuple.get(0));
									((Vector) t.overFlowSizes.get(bigPage)).add(1);
									t.NOverflows.set(bigPage, (int) t.NOverflows.get(bigPage) + 1);
									
									t.ranges.set((bigPage * 2) + 1, ((Vector) t.overFlowRanges.get(bigPage))
											.get(((Vector) t.overFlowRanges.get(bigPage)).size() - 1));
									serialize(t, tableName);
									serialize(p, bigPage, tableName);
									serializeOverflow(newPage, OFString, tableName);
									flag = true;
								} else {
									overFlowP = deserializeOverflow(OFString, tableName);

								}
							} else {

//								overFlowP.size()++;

								((Vector) t.overFlowRanges.get(bigPage)).set(OFNum * 2,
										((Tuple) overFlowP.get(0)).get(0));
								((Vector) t.overFlowRanges.get(bigPage)).set((OFNum * 2) + 1,
										((Tuple) overFlowP.get((overFlowP.size()) - 1)).get(0));
								((Vector) t.overFlowSizes.get(bigPage)).set(OFNum, overFlowP.size());
								serialize(p, bigPage, tableName);
								serialize(t, tableName);
								serializeOverflow(overFlowP, OFString, tableName);
								flag = true;
							}
						}
					} else {
						while (!flag) { // shift left
							String OFString = bigPage + "." + OFNum;
							if (overFlowP.size() == maxRows) {
								tuple = (Tuple) overFlowP.remove((0));
								((Vector) t.overFlowRanges.get(bigPage)).set(((OFNum * 2)),
										((Tuple) overFlowP.get(0)).get(0));
								serialize(p, bigPage, tableName);
								serialize(t, tableName);
								serializeOverflow(overFlowP, OFString, tableName);
								OFNum--;
								OFString = bigPage + "." + OFNum;
								overFlowP = deserializeOverflow(OFString, tableName);

							}
							overFlowP.add(tuple);
//							overFlowP.size()++;
							Collections.sort(overFlowP);
							((Vector) t.overFlowRanges.get(bigPage)).set(OFNum * 2, ((Tuple) overFlowP.get(0)).get(0));
							((Vector) t.overFlowRanges.get(bigPage)).set((OFNum * 2) + 1,
									((Tuple) overFlowP.get((overFlowP.size()) - 1)).get(0));
							((Vector) t.overFlowRanges.get(bigPage)).set(OFNum, overFlowP.size());
							serialize(p, bigPage, tableName);
							serialize(t, tableName);
							serializeOverflow(overFlowP, OFString, tableName);
							flag = true;
						}
					}
				}

			}

		}
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		
		
		
		
		try {
			
			boolean indexExists = false;
			
			
				
				
			Table t;
			t = deserialize(tableName);
			
			int indexPosition=-1;
			int minSize=99999;
			for (int i=0;i<t.indices.size();i++) {
				boolean contains = Arrays.stream((String[]) t.indices.get(i)).anyMatch(t.columns.get(0)::equals);
				if (contains) {
					indexExists=true;
					if(((String[]) t.indices.get(i)).length<minSize) {
						minSize=((String[]) t.indices.get(i)).length;
						indexPosition=i;
						
					}
				}
			}

			Index index = null;
			if (indexExists) {
				index = (Index) deserializeGeneral("Index"+indexPosition+tableName+".index");
				Enumeration<String> keys = columnNameValue.keys();
				ArrayList columnNames = new ArrayList<>();

				while (keys.hasMoreElements()) {
					String tmp = keys.nextElement().toString();
					columnNames.add(tmp);
				}
				for (int i = 0; i < columnNames.size(); i++) {
					if (!(t.columns.contains(columnNames.get(i)))) {
						throw new DBAppException();
					}
				}
				
				
				Vector bucketNames = findCKBucket(index,"Index"+indexPosition+tableName,clusteringKeyValue,tableName,t);
				Object ck = bucketNames.get(bucketNames.size()-1);
				for (int i =0; i<bucketNames.size()-1;i++) {
					String bucketName= (String) bucketNames.get(i);
				if (index.buckets.contains(bucketName)){
					page b = (page) deserializeGeneral(bucketName + ".bucket");
					Tuple searchTuple = new Tuple();
					searchTuple.add(ck);
					int bPosition = Collections.binarySearch(b, searchTuple);
					if (!(bPosition<0)) {
						String pageNum = (String) ((Vector) b.get(bPosition)).get(((Vector) b.get(bPosition)).size()-1);
						page p = (page) deserializeGeneral(tableName+"_"+pageNum+".page");
						int pPosition = Collections.binarySearch(p, searchTuple);
						Tuple oldTuple = (Tuple) p.get(pPosition);
						
						Tuple newTuple = new Tuple();
						newTuple.add(ck);
						for (int j = 1;j<t.columns.size();j++) {
							if(columnNames.contains(t.columns.get(j))) {
								newTuple.add(columnNameValue.get(t.columns.get(j)));
							}
							else {
								newTuple.add(oldTuple.get(j));
							}
						}
						
						p.set(pPosition, newTuple);
						
						deleteFromIndex(oldTuple, pageNum, tableName);
						lastPage = pageNum;
						insertIntoIndex(newTuple,pageNum,tableName,false);
						
						serialize(p,tableName+"_"+pageNum+".page");
						return;
					}
					else {
						throw new DBAppException();
					}
				}
				}
				
			}
			
			else {
			Enumeration<String> keys = columnNameValue.keys();
			ArrayList columnNames = new ArrayList<>();

			while (keys.hasMoreElements()) {
				String tmp = keys.nextElement().toString();
				columnNames.add(tmp);
			}
			for (int i = 0; i < columnNames.size(); i++) {
				if (!(t.columns.contains(columnNames.get(i)))) {
					throw new DBAppException();
				}
			}

			String type = t.ranges.get(0).getClass().getName();
			Hashtable keyTable = new Hashtable();
			Vector res = new Vector();
			switch (type) {
			case "java.lang.Integer": {
				int x = Integer.parseInt(clusteringKeyValue);
				keyTable.put(t.columns.get(0), x);
				res = CKSearch(t, tableName, keyTable);
				break;
			}
			case "java.lang.String": {

				keyTable.put(t.columns.get(0), clusteringKeyValue);
				res = CKSearch(t, tableName, keyTable);

				break;
			}
			case "java.lang.Double": {
				Double x = Double.parseDouble(clusteringKeyValue);
				keyTable.put(t.columns.get(0), x);
				res = CKSearch(t, tableName, keyTable);

				break;
			}
//    				String sDate1="31/12/1998";
//    			    Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);
//    			    System.out.println(date1);

			case "java.util.Date": {
				// System.out.println("hena");
				Date datemax = new SimpleDateFormat("yy-MM-dd").parse(clusteringKeyValue);
				keyTable.put(t.columns.get(0), datemax);
				res = CKSearch(t, tableName, keyTable);

				break;
			}

			}

			if (res != null) {
				Tuple old = (Tuple) ((page) res.get(1)).get((int) res.get(0));
				for (int i = 1; i < t.columns.size(); i++) {
					if (columnNameValue.containsKey(t.columns.get(i))) {
						old.set(i, columnNameValue.get(t.columns.get(i)));
					}
				}
				if (((String) res.get(2)).contains(".")) {
					serializeOverflow((page) res.get(1), (String) res.get(2), tableName);
				} else {
					int x = Integer.parseInt((String) res.get(2));
					serialize((page) res.get(1), x, tableName);
				}
			} else {
				throw new DBAppException();
			}
			}
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void serialize(Table t, String tableName) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + tableName + ".table");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(t);
		out.close();
		fileOut.close();
	}

	public static void serialize(Object t, String fileName) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + fileName);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(t);
		out.close();
		fileOut.close();
	}

	public static void serialize(page t, int pageNum, String tableName) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(
				"src/main/resources/data/" + tableName + "_" + pageNum + ".page");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(t);
		out.close();
		fileOut.close();
	}

	public static void serializeOverflow(page t, String pageNum, String tableName) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(
				"src/main/resources/data/" + tableName + "_" + pageNum + ".page");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(t);
		out.close();
		fileOut.close();
	}

	public static page deserialize(int pageNum, String tableName) throws IOException, ClassNotFoundException {

		FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "_" + pageNum + ".page");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		page p = (page) in.readObject();
		in.close();
		fileIn.close();
		return p;
	}

	public static Object deserializeGeneral(String fileName) throws IOException, ClassNotFoundException {

		FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + fileName);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object p = in.readObject();
		in.close();
		fileIn.close();
		return p;
	}

	public static page deserializeOverflow(String pageNum, String tableName)
			throws IOException, ClassNotFoundException {

		FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "_" + pageNum + ".page");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		page p = (page) in.readObject();
		in.close();
		fileIn.close();
		return p;
	}

	public static Table deserialize(String tableName) throws IOException, ClassNotFoundException {

		FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + ".table");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Table p = (Table) in.readObject();
		in.close();
		fileIn.close();
		return p;
	}

	public void logicElDelete(page p, int row, Table t,Tuple tuple,String pageNum,String tableName) { 
		//parameters: tuple to be deleted 
		
	

			
		try {
			 // tuple matches hashtable
				p.remove(row);
				deleteFromIndex(tuple, pageNum, tableName);
//				p.size()--;

				boolean main = false;
				if (!(pageNum.contains("."))) { // Main page
					int PageNumint = Integer.parseInt(pageNum);
					main = true;
					if (p.size() == 0) { // Delete main page

						ShiftPage(t, PageNumint, tableName, p, main, 0, true);

					} else {
						t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.get(0)).get(0));
						if ((int) t.NOverflows.get(PageNumint) == 0)
							t.ranges.set((PageNumint * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
						else
							t.ranges.set((PageNumint * 2) + 1, ((Vector) t.overFlowRanges.get(PageNumint))
									.get(((Vector) t.overFlowRanges.get(PageNumint)).size() - 1));
						
						t.sizes.set(PageNumint, p.size());
						serialize(p, PageNumint, tableName);
						serialize(t, tableName);
					}

//							Path source = Paths.get("src/main/resources/pages/" + tableName + "_" + pageNum + ".page");
//							Files.move(source, source.resolveSibling(p.));

//							t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.v.get(0)).v.get(0));
//							t.ranges.set((x * 2) + 1, ((Tuple) p.v.get(p.size() - 1)).v.get(0));
//							t.sizes.set(x, p.size());
//							serialize(p, x, tableName);
//							serialize(t, tableName);
				} else { // it's an overflow page
					String[] res = pageNum.split("\\.");
					int MainPage = Integer.parseInt(res[0]);
					int start = Integer.parseInt(res[1]);
					if (p.size() == 0) {
//					t.NOverflows.set(MainPage, (int)t.NOverflows.get(MainPage)-1);
//					((Vector)t.overFlowRanges.get(MainPage)).remove(start);
//					((Vector)t.overFlowRanges.get(MainPage)).remove(start);
//					((Vector)t.overFlowSizes.get(MainPage)).remove(start);

						ShiftPage(t, start, tableName, p, main, MainPage, true);
					} else {

						((Vector) t.overFlowRanges.get(MainPage)).set(start * 2, ((Tuple) p.get(0)).get(0));
						((Vector) t.overFlowRanges.get(MainPage)).set((start * 2) + 1,
								((Tuple) p.get(p.size() - 1)).get(0));
						t.ranges.set((MainPage * 2) + 1, ((Vector) t.overFlowRanges.get(MainPage))
								.get(((Vector) t.overFlowRanges.get(MainPage)).size() - 1));
						((Vector) t.overFlowSizes.get(MainPage)).set(start, p.size());

						serialize(t, tableName);
						serializeOverflow(p, pageNum, tableName);
					}
//				    
				}
			}
		catch(Exception e) {
			
			System.out.println("logic el delete failed");
		}
	}
		
//		else { // tuple doesnt match
//				System.out.println("Invalid Input for deletion");
//				return;
//			}

		
//			res.add(0);
//			res.add(p);
//			res.add(position / 2);
//			res.add(tableName);

		
		
		
		
	
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		boolean indexExists = false; 
		// if (!indexMatched) {
		try {
			Vector v = new Vector();
			Table t = deserialize(tableName);
			if (t.indices.size() > 0) {
				indexExists = true;
			}
			Enumeration<String> keys = columnNameValue.keys();
			ArrayList columnNames = new ArrayList<>();

			while (keys.hasMoreElements()) {
				String tmp = keys.nextElement().toString();
				columnNames.add(tmp);
			}
			
			if (!indexExists) {
			if (columnNames.contains(t.columns.get(0))) {// check if clustering key exists

				v = CKSearch(t, tableName, columnNameValue);
				if (v == null) {
					throw new DBAppException();
				}
				if ((int) v.get(0) < 0) {
					System.out.println("Not found!");
					return;
				}

				page p = (page) v.get(1);
				Tuple tuple = (Tuple) p.get((int) v.get(0));
				if (CheckNonKeys(t, tuple, columnNameValue)) { // tuple matches hashtable
					p.remove(tuple);
					if (indexExists) {
						try {
							deleteFromIndex(tuple, (String) v.get(2), tableName);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
//					p.size()--;
					String pageNum = (String) v.get(2);
					boolean main = false;
					if (!(pageNum.contains("."))) { // Main page
						int PageNumint = Integer.parseInt(pageNum);
						main = true;
						if (p.size() == 0) { // Delete main page

							ShiftPage(t, PageNumint, tableName, p, main, 0, indexExists);

						} else {
							t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.get(0)).get(0));
							if ((int) t.NOverflows.get(PageNumint) == 0)
								t.ranges.set((PageNumint * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
							else
								t.ranges.set((PageNumint * 2) + 1, ((Vector) t.overFlowRanges.get(PageNumint))
										.get(((Vector) t.overFlowRanges.get(PageNumint)).size() - 1));
							t.sizes.set(PageNumint, p.size());
							serialize(p, PageNumint, tableName);
							serialize(t, tableName);
						}

//								Path source = Paths.get("src/main/resources/pages/" + tableName + "_" + pageNum + ".page");
//								Files.move(source, source.resolveSibling(p.));

//								t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.v.get(0)).v.get(0));
//								t.ranges.set((x * 2) + 1, ((Tuple) p.v.get(p.size() - 1)).v.get(0));
//								t.sizes.set(x, p.size());
//								serialize(p, x, tableName);
//								serialize(t, tableName);
					} else { // it's an overflow page
						String[] res = pageNum.split("\\.");
						int MainPage = Integer.parseInt(res[0]);
						int start = Integer.parseInt(res[1]);
						if (p.size() == 0) {
//						t.NOverflows.set(MainPage, (int)t.NOverflows.get(MainPage)-1);
//						((Vector)t.overFlowRanges.get(MainPage)).remove(start);
//						((Vector)t.overFlowRanges.get(MainPage)).remove(start);
//						((Vector)t.overFlowSizes.get(MainPage)).remove(start);

							ShiftPage(t, start, tableName, p, main, MainPage, indexExists);
						} else {

							((Vector) t.overFlowRanges.get(MainPage)).set(start * 2, ((Tuple) p.get(0)).get(0));
							((Vector) t.overFlowRanges.get(MainPage)).set((start * 2) + 1,
									((Tuple) p.get(p.size() - 1)).get(0));
							t.ranges.set((MainPage * 2) + 1, ((Vector) t.overFlowRanges.get(MainPage))
									.get(((Vector) t.overFlowRanges.get(MainPage)).size() - 1));
							((Vector) t.overFlowSizes.get(MainPage)).set(start, p.size());

							serialize(t, tableName);
							serializeOverflow(p, pageNum, tableName);
						}
//					    
					}
				}
			}
//			else { // tuple doesnt match
//					System.out.println("Invalid Input for deletion");
//					return;
//				}

			else { // no clustering key

				int oldTableSize = t.sizes.size();

				for (int i = 0; i < t.sizes.size(); i++) { // loop over kol el main pages

					page p = deserialize(i, tableName);
					int oldPageSize = p.size();
					for (int j = 0; j < p.size(); j++) { // loop over kol main page
						if (CheckNonKeys(t, (Tuple) p.get(j), columnNameValue) == true) {
							columnNameValue.put((String) t.columns.get(0), ((Tuple) p.get(j)).get(0));
							deleteFromTable(tableName, columnNameValue);

							// check to remove
							t = deserialize(tableName); // check to remove
							if (oldTableSize > t.sizes.size()) {
								oldTableSize = t.sizes.size();
								if (oldTableSize != 0) {
									p = deserialize(i, tableName);
									oldPageSize = p.size();
									i--;
									j = 0;
									columnNameValue.remove((String) t.columns.get(0));
									break;
								}

							}

							else {
								p = deserialize(i, tableName);
								if (oldPageSize > p.size()) {
									oldPageSize = p.size();
									j--;
								}

							}
							columnNameValue.remove((String) t.columns.get(0));

						}
					}
					int OldNOverflows = (int) t.NOverflows.get(i);

					if (((Vector) t.overFlowSizes.get(i)).size() > 0) {
						for (int j = 0; j < ((Vector) t.overFlowSizes.get(i)).size(); j++) {
							page p2 = deserializeOverflow(i + "." + j, tableName);
							int oldPageSize2 = p2.size();
							for (int k = 0; k < p2.size(); k++) {
								if (CheckNonKeys(t, (Tuple) p2.get(k), columnNameValue) == true) {
									columnNameValue.put((String) t.columns.get(0), ((Tuple) p2.get(k)).get(0));
									deleteFromTable(tableName, columnNameValue);
									// check to remove

									t = deserialize(tableName); // check to remove
//								if (OldNOverflows > (int)((Vector)t.overFlowSizes.get(i)).get(j)) {
//									OldNOverflows = (int)((Vector)t.overFlowSizes.get(i)).get(j);
									if (OldNOverflows > ((Vector) t.overFlowSizes.get(i)).size()) {
										OldNOverflows = ((Vector) t.overFlowSizes.get(i)).size();
										if (OldNOverflows != 0) {
											p2 = deserializeOverflow(i + "." + j, tableName);

											oldPageSize2 = p2.size();
											j--;
											k = 0;
											columnNameValue.remove((String) t.columns.get(0));
											break;
										}

									} else {
										p2 = deserializeOverflow(i + "." + j, tableName);
										if (oldPageSize2 > p2.size()) {
											oldPageSize2 = p2.size();
											k--;
										}

									}
									columnNameValue.remove((String) t.columns.get(0));

								}
							}
						}

					}
				}
//				res.add(0);
//				res.add(p);
//				res.add(position / 2);
//				res.add(tableName);

			}
			}
			
			else { // delete using index   //delete bookmark
				Vector openedPages = new Vector();
				Vector results = new Vector();
				boolean sorted = false;
				Vector operators = new Vector();
				Tuple searchTuple = new Tuple();
				// Object searchTerm = null;
				// boolean completeParameters = false;
				Vector nonIndexedParameters = new Vector();
				Vector parameters= new Vector();
				// find right index to search with
				for (int y = 0; y < columnNames.size(); y++) {
					Vector tmp = new Vector();
					tmp.add(columnNames.get(y));
					tmp.add(columnNameValue.get((columnNames).get(y)));
					tmp.add("=");
					parameters.add(tmp);
				//
				}
				
				int rightIndex = findIndex(parameters, t);
				
				Index index = (Index) deserializeGeneral("Index" + rightIndex + tableName + ".index");
				Vector positions = new Vector(); // el cells bta3et kol el dimensions
				String indexFileName = "Index" + rightIndex + tableName;
				for (int i = 0; i < index.columnNames.size(); i++) {
					String operator = "";
					Object value = null;
					Vector v1 = new Vector();
					for (int j = 0; j < parameters.size(); j++) {
						if (((Vector) parameters.get(j)).get(0).equals(index.columnNames.get(i))) {
							value = ((Vector) parameters.get(j)).get(1);
							operator = (String) ((Vector) parameters.get(j)).get(2);
							if (i == 0) {
								// searchTerm = value;
								sorted = true;
							}

							searchTuple.add(value); // contains el parameters elly 3aleha index mesh kollohom
							operators.add(operator);

						}
					}
					if (value == null) {
						Vector tmp = new Vector();
						tmp.add(-1);
						positions.add(tmp);
						continue;
					}
					int startBucket = findDimension(index, "Index" + rightIndex + tableName, value, tableName, t,
							(String) index.columnNames.get(i));
					switch (operator) {
					case "=":
						v1.add(startBucket);
						positions.add(v1);
						break;

					case "<":
						for (int f = 1; f <= startBucket; f++) {
							v1.add(i);

						}
						positions.add(v1);
						break;
					case "<=":
						for (int f = 1; f <= startBucket; f++) {
							v1.add(i);

						}
						positions.add(v1);

						break;
					case ">":
						for (int f = startBucket; f < ((Vector) index.get(i)).size(); f++) {
							v1.add(i);

						}
						positions.add(v1);
						// break;
					case "=>":
						for (int f = startBucket; f < ((Vector) index.get(i)).size(); f++) {
							v1.add(i);

						}
						positions.add(v1);
						break;
					case "!=":
						for (int f = 1; f < ((Vector) index.get(i)).size(); f++) {
							v1.add(i);
						}
						positions.add(v1);
						break;

					}
				}

				for (int i = 0; i < parameters.size(); i++) {
					if (!index.columnNames.contains(((Vector) parameters.get(i)).get(0))) {
						nonIndexedParameters.add(parameters.get(i));
					}

				}

				for (int i = 0; i < positions.size(); i++) {
					if (((int) ((Vector) positions.get(i)).get(0)) == -1) {
						positions.remove(i);
						Vector tmp = new Vector();
						for (int j = 1; j < ((Vector) index.get(i)).size(); j++) {
							tmp.add(j);
						}
						positions.add(i, tmp);
					}

				}
				Vector bucketNames = new Vector();
				if (positions.size() == 1) {
					for (int i = 0; i < ((Vector) positions.get(0)).size(); i++) {
						String bucketName = indexFileName + "_" + ((Vector) positions.get(0)).get(i);
						bucketNames.add(bucketName);
					}
				} else {

					for (int i = 0; i < ((Vector) positions.get(0)).size(); i++) { // 2 DIMENSIONS

						for (int j = 0; j < ((Vector) positions.get(1)).size(); j++) {
							String bucketName = indexFileName + "_" + ((Vector) positions.get(0)).get(i) + "_"
									+ ((Vector) positions.get(1)).get(j);
							bucketNames.add(bucketName);
						}
					}

					if (positions.size() > 2) {
						for (int i = 2; i < positions.size(); i++) { // MORE THAN 2D
							Vector tmp = new Vector();
							for (int j = 0; j < bucketNames.size(); j++) {

								String bucketName = (String) bucketNames.get(j);
								for (int k = 0; k < ((Vector) positions.get(i)).size(); k++) {
									tmp.add(bucketName + "_" + ((Vector) positions.get(i)).get(k));
								}
							}
							bucketNames = tmp;
						}
					}
				}

				if (sorted == false) { // Sequential scanning over buckets

					// awl el method
//					System.out.println(index);
					for (int i = 0; i < bucketNames.size(); i++) {
						if (index.buckets.contains((String) bucketNames.get(i))) {
							page b = (page) deserializeGeneral((String) bucketNames.get(i) + ".bucket");

							// andOPtest
//							System.out.println(b.toString() + (String) bucketNames.get(i) + ".bucket");
//							System.out.println(b.size());
//							System.out.println("----------");

							int startIndex = 0;
							for (int i4 = 0; i4 < b.size(); i4++) {
								String tmpPageNum = "";
								boolean completeMatch = true;
								for (int l = 0; l < index.columnNames.size(); l++) {
									for (int h = 0; h < parameters.size(); h++) {
										if (((String) ((Vector) parameters.get(h)).get(0))
												.equalsIgnoreCase((String) index.columnNames.get(l))) {

											tmpPageNum = (String) ((Vector) b.get(i4)).get(((Vector) b.get(i4)).size() - 1);
											boolean compare = compareGeneral((Object) (((Vector) b.get(i4)).get(l)),
													(Object) ((Vector) parameters.get(h)).get(1),
													(String) ((Vector) parameters.get(h)).get(2));

											if (compare == false) {
												completeMatch = false;
											}
										}
									}
								}
								// shelna el 7agat elly mesh bet match el index columns mel buckets
								if (completeMatch) {
									String pageNumber = tmpPageNum;

									if (!openedPages.contains(pageNumber)) {
										openedPages.add(pageNumber);
										page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
										if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
											// searchTuple vs b.get(i4)
											int row = Collections.binarySearch(p, b.get(i4));
											boolean matching = true;
											//
											for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
												int columnPositionInTuple = t.columns
														.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
												boolean compare = compareGeneral(
														
														((Vector) p.get(row)).get(columnPositionInTuple),
														((Vector) nonIndexedParameters.get(i2)).get(1),
														(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
												if (!compare) {
													matching = false;
												}

											}
											if (matching) {
//												results.add(p.get(row));
//												p.remove(row);
//												serialize(p,tableName + "_" + pageNumber + ".page");
												//bookmark2
												//page p, int row, Table t,Tuple tuple,String pageNum,String tableName
												logicElDelete(p,row,t,(Tuple) p.get(row),pageNumber,tableName);
												startIndex--;
												// logic el delete
											}
										} else { // CK mesh mawgoud

											for (int i3 = 0; i3 < p.size(); i3++) { // debug
												// change p.size()-2 to 0
												boolean matching = true;
												for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
													int columnPositionInTuple = t.columns
															.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
													boolean compare = compareGeneral(
															
															((Vector) p.get(i3)).get(columnPositionInTuple),
															((Vector) nonIndexedParameters.get(i2)).get(1),
															(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
													if (!compare) {
														matching = false;
													}

												}
												for (int i2 = 0; i2 < parameters.size(); i2++) {
													int columnPositionInTuple = t.columns
															.indexOf(((Vector) parameters.get(i2)).get(0));
													boolean compare = compareGeneral(
															((Vector) p.get(i3)).get(columnPositionInTuple),
															((Vector) parameters.get(i2)).get(1),
															(String) ((Vector) parameters.get(i2)).get(2));
													if (!compare) {
														matching = false;
													}
												}
												if (matching) {
//													results.add(p.get(i3));
													logicElDelete(p,i3,t,(Tuple) p.get(i3),pageNumber,tableName);
													startIndex--;
													//logic el delete
//													return results; //remove after test
												}

											}
										}
									}

								}

							}
						}

					}

					// a5erha

				}
				// Parameters contain el column elly ben sort bih
				else {

					// searchTuple.add(searchTerm);
					for (int i = 0; i < bucketNames.size(); i++) {
						if (index.buckets.contains((String) bucketNames.get(i))) {
							page b = (page) deserializeGeneral((String) bucketNames.get(i) + ".bucket");
							int startIndex = -1;

							// EQUAL OPERATOR
							if (operators.get(0).equals("=")) {
								startIndex = Collections.binarySearch(b, searchTuple);
//								Tuple equalTuple = (Tuple) b.get(startIndex);
//							for(int m = startIndex; m< )
								int m = startIndex;
								boolean duplicate = true; // fy duplicates
								while (duplicate == true && startIndex >= 0 && startIndex <b.size()) {
									boolean completeMatch = true;
//								if (((Vector) b.get(startIndex)).get(0) != searchTuple.get(0)) {
									if (!compareGeneral(((Vector) b.get(startIndex)).get(0), searchTuple.get(0),
											(String) operators.get(0))) {
										duplicate = false;
									} else {
										int counter = 0; // e7na fel search tuple 3and anhy index
										for (int l = 0; l < index.columnNames.size(); l++) {

											for (int h = 0; h < parameters.size(); h++) {
												if (((String) ((Vector) parameters.get(h)).get(0))
														.equalsIgnoreCase((String) index.columnNames.get(l))) {
													// b.get(startIndex).get(l) 
													boolean compare = compareGeneral((Object) (((Vector) b.get(startIndex)).get(l)),(Object) searchTuple.get(counter),	(String) ((Vector) parameters.get(h)).get(2));
															
														
														
													if (compare == false) {
														completeMatch = false;
														startIndex++;
													} else {
														counter++;
														startIndex++; // added this
													}
												}
											}
										}
										// shelna el 7agat elly mesh bet match el index columns mel buckets
										if (completeMatch == true) {
											String pageNumber = (String) ((Vector) b.get(startIndex-1))
													.get(((Vector) b.get(startIndex-1)).size() - 1);

											if (!openedPages.contains(pageNumber)) {
												openedPages.add(pageNumber);

												page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
												if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
													// searchtuple vs b.get(startIndex)
													int row = Collections.binarySearch(p, b.get(startIndex-1));
													boolean matching = true;
													//
													for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
														int columnPositionInTuple = t.columns
																.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
														boolean compare = compareGeneral(
															
																((Vector) p.get(row)).get(columnPositionInTuple),
																((Vector) nonIndexedParameters.get(i2)).get(1),
																(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
														if (!compare) {
															matching = false;
														}

													}
													if (matching) {
//														results.add(p.get(row));
														// logic el delete
														logicElDelete(p,row,t,(Tuple) p.get(row),pageNumber,tableName);
														startIndex--;
													}
												} else { // CK mesh mawgoud

													for (int i3 = 0; i3 < p.size(); i3++) { // ck not present so we have to loop over the parameters too
															//change p.size()-2 to zero
														boolean matching = true;  //testing case
														for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
															int columnPositionInTuple = t.columns.indexOf(
																	((Vector) nonIndexedParameters.get(i2)).get(0));
															boolean compare = compareGeneral(
																	
																	((Vector) p.get(i3)).get(columnPositionInTuple),
																	((Vector) nonIndexedParameters.get(i2)).get(1),
																	(String) ((Vector) nonIndexedParameters.get(i2))
																			.get(2));
															if (!compare) {
																matching = false;
															}
														}

														for (int i2 = 0; i2 < parameters.size(); i2++) {   //added this
															int columnPositionInTuple = t.columns
																	.indexOf(((Vector) parameters.get(i2)).get(0));
															boolean compare = compareGeneral(
																
																	((Vector) p.get(i3)).get(columnPositionInTuple),
																	((Vector) parameters.get(i2)).get(1),
																	(String) ((Vector) parameters.get(i2)).get(2));
															if (!compare) {
																matching = false;
															}
														}
														if (matching) {
//															results.add(p.get(i3));
															//logic el delete
															logicElDelete(p,i3,t,(Tuple) p.get(i3),pageNumber,tableName);
															startIndex--;
//															return results;  // remove after test

														}
													}
												}
											}
//											else {
//												continue;
//											}

										}
									}
									
								}
							}
							// ay operator mesh EQUAL
//							else {
//								for (int i4 = 0; i4 < b.size(); i4++) {
//									boolean completeMatch = true;
//									for (int l = 0; l < index.columnNames.size(); l++) {
//										for (int h = 0; h < parameters.size(); h++) {
//											if (((String) ((Vector) parameters.get(h)).get(0))
//													.equalsIgnoreCase((String) index.columnNames.get(l))) {
//												boolean compare = compareGeneral((Object) (b.get(startIndex)),
//														(Object) searchTuple.get((int) b.get(i4)),
//														(String) ((Vector) parameters.get(h)).get(2));
//												if (compare == false) {
//													completeMatch = false;
//												}
//											}
//										}
//									}
//									// shelna el 7agat elly mesh bet match el index columns mel buckets
//									if (completeMatch) {
//										String pageNumber = (String) ((Vector) b.get(startIndex))
//												.get(((Vector) b.get(startIndex)).size() - 1);
//										if (!openedPages.contains(pageNumber)) {
//											openedPages.add(pageNumber);
//											page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
//											if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
//												int row = Collections.binarySearch(p, searchTuple);
//												boolean matching = true;
//												//
//												for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
//													int columnPositionInTuple = t.columns
//															.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
//													boolean compare = compareGeneral(
//														
//															((Vector) p.get(row)).get(columnPositionInTuple),
//															((Vector) nonIndexedParameters.get(i2)).get(1),
//															(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
//													if (!compare) {
//														matching = false;
//													}
//
//												}
//												if (matching) {
//													results.add(p.get(row));
//												}
//											} else { // CK mesh mawgoud
//												boolean matching = true;
//
//												for (int i3 = 0; i3 < p.size(); i3++) {
//													for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
//														int columnPositionInTuple = t.columns
//																.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
//														boolean compare = compareGeneral(
//																
//																((Vector) p.get(i3)).get(columnPositionInTuple),
//																((Vector) nonIndexedParameters.get(i2)).get(1),
//																(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
//														if (!compare) {
//															matching = false;
//														}
//
//													}
//													if (matching) {
//														results.add(p.get(i3));
//													}
//												}
//											}
//										}
//
//									}
//
//								}
//							}
						}
					}
				}
			}
				
//				if (columnNames.contains(t.columns.get(0))) {// check if clustering key exists
//
//					v = CKSearch(t, tableName, columnNameValue);
//					if (v == null) {
//						throw new DBAppException();
//					}
//					if ((int) v.get(0) < 0) {
//						System.out.println("Not found!");
//						return;
//					}
//
//					page p = (page) v.get(1);
//					Tuple tuple = (Tuple) p.get((int) v.get(0));
//					if (CheckNonKeys(t, tuple, columnNameValue)) { // tuple matches hashtable
//						p.remove(tuple);
//						if (indexExists) {
//							try {
//								deleteFromIndex(tuple, (String) v.get(2), tableName);
//							} catch (ParseException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
//						}
////						p.size()--;
//						String pageNum = (String) v.get(2);
//						boolean main = false;
//						if (!(pageNum.contains("."))) { // Main page
//							int PageNumint = Integer.parseInt(pageNum);
//							main = true;
//							if (p.size() == 0) { // Delete main page
//
//								ShiftPage(t, PageNumint, tableName, p, main, 0, indexExists);
//
//							} else {
//								t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.get(0)).get(0));
//								if ((int) t.NOverflows.get(PageNumint) == 0)
//									t.ranges.set((PageNumint * 2) + 1, ((Tuple) p.get(p.size() - 1)).get(0));
//								else
//									t.ranges.set((PageNumint * 2) + 1, ((Vector) t.overFlowRanges.get(PageNumint))
//											.get(((Vector) t.overFlowRanges.get(PageNumint)).size() - 1));
//								t.sizes.set(PageNumint, p.size());
//								serialize(p, PageNumint, tableName);
//								serialize(t, tableName);
//							}
//
////									Path source = Paths.get("src/main/resources/pages/" + tableName + "_" + pageNum + ".page");
////									Files.move(source, source.resolveSibling(p.));
//
////									t.ranges.set(Integer.parseInt(pageNum) * 2, ((Tuple) p.v.get(0)).v.get(0));
////									t.ranges.set((x * 2) + 1, ((Tuple) p.v.get(p.size() - 1)).v.get(0));
////									t.sizes.set(x, p.size());
////									serialize(p, x, tableName);
////									serialize(t, tableName);
//						} else { // it's an overflow page
//							String[] res = pageNum.split("\\.");
//							int MainPage = Integer.parseInt(res[0]);
//							int start = Integer.parseInt(res[1]);
//							if (p.size() == 0) {
////							t.NOverflows.set(MainPage, (int)t.NOverflows.get(MainPage)-1);
////							((Vector)t.overFlowRanges.get(MainPage)).remove(start);
////							((Vector)t.overFlowRanges.get(MainPage)).remove(start);
////							((Vector)t.overFlowSizes.get(MainPage)).remove(start);
//
//								ShiftPage(t, start, tableName, p, main, MainPage, indexExists);
//							} else {
//
//								((Vector) t.overFlowRanges.get(MainPage)).set(start * 2, ((Tuple) p.get(0)).get(0));
//								((Vector) t.overFlowRanges.get(MainPage)).set((start * 2) + 1,
//										((Tuple) p.get(p.size() - 1)).get(0));
//								t.ranges.set((MainPage * 2) + 1, ((Vector) t.overFlowRanges.get(MainPage))
//										.get(((Vector) t.overFlowRanges.get(MainPage)).size() - 1));
//								((Vector) t.overFlowSizes.get(MainPage)).set(start, p.size());
//
//								serialize(t, tableName);
//								serializeOverflow(p, pageNum, tableName);
//							}
////						    
//						}
//					}
//				}
////				else { // tuple doesnt match
////						System.out.println("Invalid Input for deletion");
////						return;
////					}
//
//				else { // no clustering key  
//
//					int oldTableSize = t.sizes.size();
//
//					for (int i = 0; i < t.sizes.size(); i++) { // loop over kol el main pages
//
//						page p = deserialize(i, tableName);
//						int oldPageSize = p.size();
//						for (int j = 0; j < p.size(); j++) { // loop over kol main page
//							if (CheckNonKeys(t, (Tuple) p.get(j), columnNameValue) == true) {
//								columnNameValue.put((String) t.columns.get(0), ((Tuple) p.get(j)).get(0));
//								deleteFromTable(tableName, columnNameValue);
//
//								// check to remove
//								t = deserialize(tableName); // check to remove
//								if (oldTableSize > t.sizes.size()) {
//									oldTableSize = t.sizes.size();
//									if (oldTableSize != 0) {
//										p = deserialize(i, tableName);
//										oldPageSize = p.size();
//										i--;
//										j = 0;
//										columnNameValue.remove((String) t.columns.get(0));
//										break;
//									}
//
//								}
//
//								else {
//									p = deserialize(i, tableName);
//									if (oldPageSize > p.size()) {
//										oldPageSize = p.size();
//										j--;
//									}
//
//								}
//								columnNameValue.remove((String) t.columns.get(0));
//
//							}
//						}
//						int OldNOverflows = (int) t.NOverflows.get(i);
//
//						if (((Vector) t.overFlowSizes.get(i)).size() > 0) {
//							for (int j = 0; j < ((Vector) t.overFlowSizes.get(i)).size(); j++) {
//								page p2 = deserializeOverflow(i + "." + j, tableName);
//								int oldPageSize2 = p2.size();
//								for (int k = 0; k < p2.size(); k++) {
//									if (CheckNonKeys(t, (Tuple) p2.get(k), columnNameValue) == true) {
//										columnNameValue.put((String) t.columns.get(0), ((Tuple) p2.get(k)).get(0));
//										deleteFromTable(tableName, columnNameValue);
//										// check to remove
//
//										t = deserialize(tableName); // check to remove
////									if (OldNOverflows > (int)((Vector)t.overFlowSizes.get(i)).get(j)) {
////										OldNOverflows = (int)((Vector)t.overFlowSizes.get(i)).get(j);
//										if (OldNOverflows > ((Vector) t.overFlowSizes.get(i)).size()) {
//											OldNOverflows = ((Vector) t.overFlowSizes.get(i)).size();
//											if (OldNOverflows != 0) {
//												p2 = deserializeOverflow(i + "." + j, tableName);
//
//												oldPageSize2 = p2.size();
//												j--;
//												k = 0;
//												columnNameValue.remove((String) t.columns.get(0));
//												break;
//											}
//
//										} else {
//											p2 = deserializeOverflow(i + "." + j, tableName);
//											if (oldPageSize2 > p2.size()) {
//												oldPageSize2 = p2.size();
//												k--;
//											}
//
//										}
//										columnNameValue.remove((String) t.columns.get(0));
//
//									}
//								}
//							}
//
//						}
//					}
////					res.add(0);
////					res.add(p);
////					res.add(position / 2);
////					res.add(tableName);
//
//				}
//			}
//		}
		}
			catch (

		ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// }
 catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void ShiftPage(Table t, int start, String tableName, page p, boolean main, int MainPage,
			boolean indexExists) throws IOException, ClassNotFoundException {
		if (main == true) { // Main Page
			if (start == t.sizes.size() - 1) { // Last page Main
				if ((int) t.NOverflows.get(start) == 0) { // No overflow
					File f1 = new File("src/main/resources/data/" + tableName + "_" + start + ".page");
					f1.delete();
					t.NOverflows.remove(start);
					t.nPages--;
					t.ranges.remove(start * 2);
					t.ranges.remove(start * 2);
					t.sizes.remove(start);

					t.overFlowRanges.remove(start);
//					t.overFlowRanges.remove(start);
					t.overFlowSizes.remove(start);
					serialize(t, tableName);
					return;
				} else { // overflow pages for last page
					int NOverflows = (int) t.NOverflows.get(start);
					if (NOverflows == 1) {
						File f1 = new File("src/main/resources/data/" + tableName + "_" + start + ".0" + ".page");
						File f2 = new File("src/main/resources/data/" + tableName + "_" + start + ".page");
						t.ranges.set(start * 2, ((Vector) t.overFlowRanges.get(start)).get(0));

						t.NOverflows.set(start, (int) t.NOverflows.get(start) - 1);
						t.sizes.set(start, ((Vector) t.overFlowSizes.get(start)).get(0));

						((Vector) t.overFlowRanges.get(start)).remove(0);
						((Vector) t.overFlowRanges.get(start)).remove(0);
						((Vector) t.overFlowSizes.get(start)).remove(0);
//					page p2 = deserializeOverflow(start+".0", tableName);
//					p2.overFlowRanges=p.overFlowRanges;
//					p2.overFlowSizes=p.overFlowSizes;
//					p2.overFlowRanges.remove(0);
//					p2.overFlowSizes.remove(0);
//					serializeOverflow(p2,"0.0",tableName);
						serialize(t, tableName);
						f2.delete();
						f1.renameTo(f2);
						updateIndex(start + ".0", start + "", t, tableName);

////				System.out.println();
////				f2.delete();
					} else {
						for (int i = 0; i < NOverflows; i++) {
							if (i == 0) {
								File f1 = new File(
										"src/main/resources/data/" + tableName + "_" + start + ".0" + ".page");
								File f2 = new File("src/main/resources/data/" + tableName + "_" + start + ".page");
								t.ranges.set(start * 2, ((Vector) t.overFlowRanges.get(start)).get(0));
								t.NOverflows.set(start, (int) t.NOverflows.get(start) - 1);
								t.sizes.set(start, ((Vector) t.overFlowSizes.get(start)).get(0));

								((Vector) t.overFlowRanges.get(start)).remove(0);
								((Vector) t.overFlowRanges.get(start)).remove(0);
								((Vector) t.overFlowSizes.get(start)).remove(0);

//								page p2 = deserializeOverflow(start + ".0", tableName);
//								p2.overFlowRanges = p.overFlowRanges;
//								p2.overFlowSizes = p.overFlowSizes;
//								p2.overFlowRanges.remove(0);
//								p2.overFlowSizes.remove(0);
//								serializeOverflow(p2, "0.0", tableName);
								serialize(t, tableName);
								f2.delete();
								f1.renameTo(f2);
								updateIndex(start + ".0", start + "", t, tableName);
							} else {
								File f1 = new File(
										"src/main/resources/data/" + tableName + "_" + start + "." + (i - 1) + ".page");
								File f2 = new File(
										"src/main/resources/data/" + tableName + "_" + start + "." + (i) + ".page");
								f1.delete();
								f2.renameTo(f1);
								updateIndex(start + "." + (i) + ".0", start + "." + (i - 1), t, tableName);
							}
						}

					}

				}

			}

			else { // not last page
				if ((int) t.NOverflows.get(start) > 0) { // with overflow
					int NOverflows = (int) t.NOverflows.get(start);
					for (int i = 0; i < (int) NOverflows; i++) { // not last page with overflow
						if (i == 0) {
							File f1 = new File("src/main/resources/data/" + tableName + "_" + start + ".0" + ".page");
							File f2 = new File("src/main/resources/data/" + tableName + "_" + start + ".page");
							t.ranges.set(start * 2, ((Vector) t.overFlowRanges.get(MainPage)).get(0));
							t.NOverflows.set(start, (int) t.NOverflows.get(start) - 1);
							t.sizes.set(start, ((Vector) t.overFlowSizes.get(MainPage)).get(0));

							((Vector) t.overFlowRanges.get(MainPage)).remove(0);
							((Vector) t.overFlowRanges.get(MainPage)).remove(0);
							((Vector) t.overFlowSizes.get(MainPage)).remove(0);

//							page p2 = deserializeOverflow(start + ".0", tableName);
//							p2.overFlowRanges = p.overFlowRanges;
//							p2.overFlowSizes = p.overFlowSizes;
//							p2.overFlowRanges.remove(0);
//							p2.overFlowSizes.remove(0);
//							serializeOverflow(p2, "0.0", tableName);
							serialize(t, tableName);
							f2.delete();
							f1.renameTo(f2);
							updateIndex(start + ".0", start + "", t, tableName);
						} else {
							File f1 = new File(
									"src/main/resources/data/" + tableName + "_" + start + "." + (i - 1) + ".page");
							File f2 = new File(
									"src/main/resources/data/" + tableName + "_" + start + "." + (i) + ".page");
							f1.delete();
							f2.renameTo(f1);
							updateIndex(start + "." + (i) + ".0", start + "." + (i - 1), t, tableName);
						}
					}

				} else { // not last page with no overflows

					for (int i = start; i < t.sizes.size() - 1; i++) {
						File f1 = new File("src/main/resources/data/" + tableName + "_" + start + ".page");
						File f2 = new File("src/main/resources/data/" + tableName + "_" + (start + 1) + ".page");
						f1.delete();
						f2.renameTo(f1);
						updateIndex((start + 1) + "", start + ".", t, tableName);
						int NOverflows2 = (int) t.NOverflows.get(start + 1);
						if (NOverflows2 > 0) {
							for (int j = 0; j < NOverflows2; j++) {
								File f11 = new File(
										"src/main/resources/data/" + tableName + "_" + start + "." + j + ".page");
								File f22 = new File(
										"src/main/resources/data/" + tableName + "_" + (start + 1) + "." + j + ".page");
								f11.delete();
								f22.renameTo(f11);
								updateIndex((start + 1) + "." + j, start + "." + j, t, tableName);
							}

						}

					}
					t.nPages--;
					t.ranges.remove(start * 2);
					t.ranges.remove(start * 2);
					t.sizes.remove(start);
					t.NOverflows.remove(start);
					t.overFlowRanges.remove(start);
//					t.overFlowRanges.remove(start);
					t.overFlowSizes.remove(start);
					serialize(t, tableName);
				}

			}
		}

		else { // Overflow page

			int NOverflows = (int) t.NOverflows.get(MainPage);
			if (NOverflows == 1) {
				File f22 = new File("src/main/resources/data/" + tableName + "_" + MainPage + "." + "0" + ".page");
				f22.delete();

				((Vector) t.overFlowRanges.get(MainPage)).remove(start);
				((Vector) t.overFlowSizes.get(MainPage)).remove(start);

				((Vector) t.overFlowRanges.get(MainPage)).remove(start);

				page og = deserialize(MainPage, tableName);
				t.ranges.set((MainPage * 2) + 1, ((Tuple) og.get(og.size() - 1)).get(0));
				t.NOverflows.set(MainPage, (int) t.NOverflows.get(MainPage) - 1);

				serialize(t, tableName);
			} else {
				for (int i = start; i < NOverflows; i++) {
					if (i == NOverflows - 1) {
						File f22 = new File(
								"src/main/resources/data/" + tableName + "_" + MainPage + "." + i + ".page");
						f22.delete();
						((Vector) t.overFlowRanges.get(MainPage)).remove(start * 2);
						((Vector) t.overFlowRanges.get(MainPage)).remove(start * 2);
						((Vector) t.overFlowSizes.get(MainPage)).remove(start);
						Vector v = (Vector) t.overFlowRanges.get(MainPage);
						t.ranges.set((MainPage * 2) + 1, v.get(v.size() - 1));
						t.NOverflows.set(MainPage, (int) t.NOverflows.get(MainPage) - 1);
						serialize(t, tableName);
					} else {
						File f11 = new File(
								"src/main/resources/data/" + tableName + "_" + MainPage + "." + (i) + ".page");
						File f22 = new File(
								"src/main/resources/data/" + tableName + "_" + MainPage + "." + (i + 1) + ".page");
						f11.delete();
						f22.renameTo(f11);
						updateIndex(MainPage + "." + (i + 1), MainPage + "." + (i), t, tableName);
					}
				}

			}
		}

	}

	private static void updateIndex(String oldPage, String newPage, Table table, String tableName) //updates page numbers when shifting happens
			throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub

		for (int i = 0; i < table.indices.size(); i++) {

			Index index = (Index) deserializeGeneral("Index" + i + tableName + ".index");
			// Index0students.index
			for (int j = 0; j < index.buckets.size(); j++) {
				page b = (page) deserializeGeneral(index.buckets.get(j) + ".bucket");

				for (int k = 0; k < b.size(); k++) {
					if (((Tuple) b.get(k)).get(((Tuple) b.get(k)).size() - 1).equals(oldPage)) {
						((Tuple) b.get(k)).set(((Tuple) b.get(k)).size() - 1, newPage);

					}
				}
				serialize(b, index.buckets.get(j) + ".bucket");
			}

		}

	}
	
	

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0]._strTableName = "Student";
//		arrSQLTerms[0]._strColumnName= "name";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "John Noor";

//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName= "gpa";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new Double( 1.5 );

//		String[]strarrOperators = new String[1];
//		strarrOperators[0] = "OR";

		Vector results = new Vector();
		Vector parameters = new Vector();
		
		//SELECT BOOKMARK

		try {

			Table t = deserialize(sqlTerms[0]._strTableName);
			String columnName = "";
			String operator = "";
			Object objValue;
			// (a,1,"=")
			Vector andedParameters = new Vector();

			for (int y = 0; y < sqlTerms.length; y++) {
				Vector tmp = new Vector();
				tmp.add(sqlTerms[y]._strColumnName);
				tmp.add(sqlTerms[y]._objValue);
				tmp.add(sqlTerms[y]._strOperator);
				parameters.add(tmp);

				if (y == sqlTerms.length - 1) {
					andedParameters.add(parameters);
					parameters = new Vector();
				}
				if (y != sqlTerms.length - 1 && !arrayOperators[y].equalsIgnoreCase("and")) {
					// results = andOP(parameters, t, tableName);
					andedParameters.add(parameters);
					parameters = new Vector();
				}
			}
			try {
				for (int i = 0; i < andedParameters.size(); i++) {
					results.add(andOP((Vector) andedParameters.get(i), t, sqlTerms[0]._strTableName));
				}
				Vector remainingParameters = new Vector();
				for (int i = 0; i < arrayOperators.length; i++) {
					if (!arrayOperators[i].equalsIgnoreCase("and")) {
						remainingParameters.add(arrayOperators[i].toLowerCase());
					}

				}
				Vector orTuples = new Vector();
				Vector xorTuples = new Vector();
				orTuples.add(results.get(0)); // 1st tuple from the result set
				for (int i = 0; i < remainingParameters.size(); i++) {
					if (i == remainingParameters.size() - 1) {
						if (((String) remainingParameters.get(i)).equalsIgnoreCase("or")) {
							orTuples.add(results.get(i + 1));
							Vector tmp = removeDuplicates(orTuples);
							xorTuples.add(tmp);
						} else { // xor mesh or
							Vector tmp = removeDuplicates(orTuples);
							xorTuples.add(tmp);
							orTuples = new Vector();
							xorTuples.add(results.get(i + 1));
						}
					} else {
						if (((String) remainingParameters.get(i)).equalsIgnoreCase("or")) {
							orTuples.add(results.get(i + 1));
						} else { // xor or fel nos mesh or w mesh a5er parameter
							Vector tmp = removeDuplicates(orTuples);
							xorTuples.add(tmp);
							orTuples = new Vector();
							orTuples.add(results.get(i + 1));
						}
					}
				}
				if (remainingParameters.size() == 0) {
					Iterator resultSet = orTuples.iterator();
					return resultSet;
				}
				if (remainingParameters.contains("xor")) {
					Vector finalResult = xor(xorTuples);
					Iterator resultSet = finalResult.iterator();
					return resultSet;
				} else {
					Iterator resultSet = xorTuples.iterator();
					return resultSet;
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	private Vector xor(Vector xorTuples) {
		// TODO Auto-generated method stub
		Vector tmp = new Vector();
		for (int i = 0; i < xorTuples.size(); i++) {
			for (int j = 0; j < ((Vector) xorTuples.get(i)).size(); j++) {
				tmp.add(((Vector) xorTuples.get(i)).get(j));

			}

		}
		Collections.sort(tmp);
		Vector results = new Vector();
		int counter = 1;
		Tuple tmpTuple = new Tuple();
		if (tmp.size() == 0) {
			return new Vector();
		}
		tmpTuple = (Tuple) tmp.get(0);
		for (int i = 1; i < tmp.size(); i++) {
			
			if (i == tmp.size() - 1) {
				if (tmpTuple.equals(tmp.get(i))) {// check for same consecutive tuples
					continue;
				} else {// pattern broken
					results.add(tmp.get(i));

				}
			}
			if (tmpTuple.equals(tmp.get(i))) {// check for same consecutive tuples
				counter++;
			} else {
				if (counter % 2 == 1) { // pattern broken
					results.add(tmpTuple);
					counter = 1;
					tmpTuple = (Tuple) tmp.get(i);
				}
				counter = 1;
				tmpTuple = (Tuple) tmp.get(i);

			}
			

		}
		return results;
	}

	private Vector removeDuplicates(Vector orTuples) {
		// TODO Auto-generated method stub
		Vector tmp = new Vector();
		for (int i = 0; i < orTuples.size(); i++) {
			for (int j = 0; j < ((Vector) orTuples.get(i)).size(); j++) {
				tmp.add(((Vector) orTuples.get(i)).get(j));

			}

		}
		Collections.sort(tmp);
		Vector results = new Vector();
		Tuple tmpTuple = new Tuple();
		if (tmp.size() == 0) {
			return orTuples;
		}
		tmpTuple = (Tuple) tmp.get(0);
//		results.add(tmpTuple);
		for (int i = 1; i < tmp.size(); i++) {
			
			if (i == tmp.size() - 1) {
				if (tmpTuple.equals(tmp.get(i))) {// check for same consecutive tuples
					continue;
				} else {// pattern broken
					results.add(tmp.get(i));

				}
			}
			
			if (tmpTuple.equals(tmp.get(i))) {// check for same consecutive tuples
				continue;
			} else {// pattern broken
				results.add(tmpTuple);

				tmpTuple = (Tuple) tmp.get(i);

			}
			if (i == tmp.size() - 1) {
				if (tmpTuple.equals(tmp.get(i))) {// check for same consecutive tuples
					continue;
				} else {// pattern broken
					results.add(tmp.get(i));

					tmpTuple = (Tuple) tmp.get(i);

				}
			}

		}
		return results;

	}

	public static int findIndex(Vector parameters, Table t) {
		if (t.indices.size() > 0) {
			int max = -9999;
			int rightIndex = -9999;

			Vector paramColumnNames = new Vector();
			for (int i = 0; i < parameters.size(); i++) // columns in parameters
			{
				paramColumnNames.add(((Vector) parameters.get(i)).get(0));
			}
			for (int i = 0; i < t.indices.size(); i++) {
				int score = -1;
				String[] indexColumns = (String[]) t.indices.get(i);
				for (int j = 0; j < indexColumns.length; j++) {
					if (paramColumnNames.contains(indexColumns[j])) {
						score++;
					} else {
						score--;
					}
				}
				if (max < score) {
					max = score;
					rightIndex = i;
				}
			}

			return rightIndex;
		}

		else { // no index to do operation with
			return -1;
		}
	}

	public static boolean compareGeneral(Object x, Object y, String operator) {
		String type = x.getClass().getName();
		// String type = y.getClass().getName();
		int res = 2222;
		switch (type) {
		case "java.lang.Integer":
			if ((int) x < (int) y) {
				res = -1;
			} else {
				if ((int) x > (int) y)
					res = 1;
				else
					res = 0;
			}

			break;

		case "java.lang.String":
			res = (((String) x).compareToIgnoreCase((String) y)); {

		}
			break;

		case "java.lang.Double":
			if ((double) x < (double) y) {
				res = -1;
			} else if ((double) x > (double) y) {
				res = 1;
			} else
				res = 0;
			break;

		case "java.util.Date":

//				String xx=x+"";
//				String yy=y+"";
			Date indate;
			indate = (Date) x;
			Date date2 = (Date) y;

			res = indate.compareTo(date2);

			break;
		}
		switch (operator) {
		case "=":
//			v.add(startBucket);
//			positions.add(v);
			if (res == 0) {
				return true;
			} else {
				return false;
			}

		case "<":
			if (res < 0) {
				return true;
			} else {
				return false;
			}
			// break;
		case "<=":
			if (res <= 0) {
				return true;
			} else {
				return false;
			}
			// break;
		case ">":
			if (res > 0) {
				return true;
			} else {
				return false;
			}
			// break;
		case "=>":  //check
			if (res >= 0) {
				return true;
			} else {
				return false;
			}
			// break;
		case "!=":
			if (res != 0) {
				return true;
			} else {
				return false;
			}
			// break;
		}
		return false;
	}

	public static Vector andOP(Vector parameters, Table t, String tableName)
			throws ClassNotFoundException, IOException, ParseException {  //andOP BOOKMARK
		// ( (a,1,"=") (name,ahmed,"<") )
//		>, >=, <, <=, != or =
		Vector openedPages = new Vector();
		Vector results = new Vector();
		boolean sorted = false;
		Vector operators = new Vector();
		Tuple searchTuple = new Tuple();
		// Object searchTerm = null;
		// boolean completeParameters = false;
		Vector nonIndexedParameters = new Vector();

		int rightIndex = findIndex(parameters, t);

		if (rightIndex == -1) { // no index to search with

			// POSSIBLE OPTIMIZATION: DIFFERENTIATE CASES CK OR NO CK
			for (int i = 0; i < t.nPages; i++) {
				page p = deserialize(i, tableName);
				for (int j = 0; j < p.size(); j++) { // inside each page
					boolean matching = true;
					for (int i2 = 0; i2 < parameters.size(); i2++) {
						int columnPositionInTuple = t.columns.indexOf(((Vector) parameters.get(i2)).get(0));
						boolean compare = compareGeneral(
								((Vector) p.get(j)).get(columnPositionInTuple),
								((Vector) parameters.get(i2)).get(1),
								(String) ((Vector) parameters.get(i2)).get(2));
						if (!compare) {
							matching = false;
						}
					}
					if (matching) {
						results.add(p.get(j));
					}
				}
				// loop over overflows
				for (int j2 = 0; j2 < (int) t.NOverflows.get(i); j2++) {
					page p1 = deserializeOverflow(i + "." + j2, tableName);
					for (int j = 0; j < p.size(); j++) { // inside each overflow
						boolean matching = true;
						for (int i2 = 0; i2 < parameters.size(); i2++) {
							int columnPositionInTuple = t.columns.indexOf(((Vector) parameters.get(i2)).get(0));
							boolean compare = compareGeneral(
									((Vector) p1.get(j)).get(columnPositionInTuple),
									((Vector) parameters.get(i2)).get(1),
									(String) ((Vector) parameters.get(i2)).get(2));
							if (!compare) {
								matching = false;
							}
						}
						if (matching) {
							results.add(p1.get(j));
						}
					}
				}
			}
		} else { // index
			Index index = (Index) deserializeGeneral("Index" + rightIndex + tableName + ".index");
			Vector positions = new Vector(); // el cells bta3et kol el dimensions
			String indexFileName = "Index" + rightIndex + tableName;
			for (int i = 0; i < index.columnNames.size(); i++) {
				String operator = "";
				Object value = null;
				Vector v = new Vector();
				for (int j = 0; j < parameters.size(); j++) {
					if (((Vector) parameters.get(j)).get(0).equals(index.columnNames.get(i))) {
						value = ((Vector) parameters.get(j)).get(1);
						operator = (String) ((Vector) parameters.get(j)).get(2);
						if (i == 0) {
							// searchTerm = value;
							sorted = true;
						}

						searchTuple.add(value); // contains el parameters elly 3aleha index mesh kollohom
						operators.add(operator);

					}
				}
				if (value == null) {
					Vector tmp = new Vector();
					tmp.add(-1);
					positions.add(tmp);
					continue;
				}
				int startBucket = findDimension(index, "Index" + rightIndex + tableName, value, tableName, t,
						(String) index.columnNames.get(i));
				switch (operator) {
				case "=":
					v.add(startBucket);
					positions.add(v);
					break;

				case "<":
					for (int f = 1; f <= startBucket; f++) {
						v.add(i);

					}
					positions.add(v);
					break;
				case "<=":
					for (int f = 1; f <= startBucket; f++) {
						v.add(i);

					}
					positions.add(v);

					break;
				case ">":
					for (int f = startBucket; f < ((Vector) index.get(i)).size(); f++) {
						v.add(i);

					}
					positions.add(v);
					// break;
				case "=>":
					for (int f = startBucket; f < ((Vector) index.get(i)).size(); f++) {
						v.add(i);

					}
					positions.add(v);
					break;
				case "!=":
					for (int f = 1; f < ((Vector) index.get(i)).size(); f++) {
						v.add(i);
					}
					positions.add(v);
					break;

				}
			}

			for (int i = 0; i < parameters.size(); i++) {
				if (!index.columnNames.contains(((Vector) parameters.get(i)).get(0))) {
					nonIndexedParameters.add(parameters.get(i));
				}

			}

			for (int i = 0; i < positions.size(); i++) {
				if (((int) ((Vector) positions.get(i)).get(0)) == -1) {
					positions.remove(i);
					Vector tmp = new Vector();
					for (int j = 1; j < ((Vector) index.get(i)).size(); j++) {
						tmp.add(j);
					}
					positions.add(i, tmp);
				}

			}
			Vector bucketNames = new Vector();
			if (positions.size() == 1) {
				for (int i = 0; i < ((Vector) positions.get(0)).size(); i++) {
					String bucketName = indexFileName + "_" + ((Vector) positions.get(0)).get(i);
					bucketNames.add(bucketName);
				}
			} else {

				for (int i = 0; i < ((Vector) positions.get(0)).size(); i++) { // 2 DIMENSIONS

					for (int j = 0; j < ((Vector) positions.get(1)).size(); j++) {
						String bucketName = indexFileName + "_" + ((Vector) positions.get(0)).get(i) + "_"
								+ ((Vector) positions.get(1)).get(j);
						bucketNames.add(bucketName);
					}
				}

				if (positions.size() > 2) {
					for (int i = 2; i < positions.size(); i++) { // MORE THAN 2D
						Vector tmp = new Vector();
						for (int j = 0; j < bucketNames.size(); j++) {

							String bucketName = (String) bucketNames.get(j);
							for (int k = 0; k < ((Vector) positions.get(i)).size(); k++) {
								tmp.add(bucketName + "_" + ((Vector) positions.get(i)).get(k));
							}
						}
						bucketNames = tmp;
					}
				}
			}

			if (sorted == false) { // Sequential scanning over buckets

				// awl el method
//				System.out.println(index);
				for (int i = 0; i < bucketNames.size(); i++) {
					if (index.buckets.contains((String) bucketNames.get(i))) {
						page b = (page) deserializeGeneral((String) bucketNames.get(i) + ".bucket");

						// andOPtest
//						System.out.println(b.toString() + (String) bucketNames.get(i) + ".bucket");
//						System.out.println(b.size());
//						System.out.println("----------");

						int startIndex = 0;
						for (int i4 = 0; i4 < b.size(); i4++) {
							String tmpPageNum = "";
							boolean completeMatch = true;
							for (int l = 0; l < index.columnNames.size(); l++) {
								for (int h = 0; h < parameters.size(); h++) {
									if (((String) ((Vector) parameters.get(h)).get(0))
											.equalsIgnoreCase((String) index.columnNames.get(l))) {

										tmpPageNum = (String) ((Vector) b.get(i4)).get(((Vector) b.get(i4)).size() - 1);
										boolean compare = compareGeneral((Object) (((Vector) b.get(i4)).get(l)),
												(Object) ((Vector) parameters.get(h)).get(1),
												(String) ((Vector) parameters.get(h)).get(2));

										if (compare == false) {
											completeMatch = false;
										}
									}
								}
							}
							// shelna el 7agat elly mesh bet match el index columns mel buckets
							if (completeMatch) {
								String pageNumber = tmpPageNum;

								if (!openedPages.contains(pageNumber)) {
									openedPages.add(pageNumber);
									page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
									if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
										int row = Collections.binarySearch(p, searchTuple);
										boolean matching = true;
										//
										for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
											int columnPositionInTuple = t.columns
													.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
											boolean compare = compareGeneral(
													
													((Vector) p.get(row)).get(columnPositionInTuple),
													((Vector) nonIndexedParameters.get(i2)).get(1),
													(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
											if (!compare) {
												matching = false;
											}

										}
										if (matching) {
											results.add(p.get(row));
										}
									} else { // CK mesh mawgoud

										for (int i3 = 0; i3 < p.size(); i3++) { // debug
											// change p.size()-2 to 0
											boolean matching = true;
											for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
												int columnPositionInTuple = t.columns
														.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
												boolean compare = compareGeneral(
														
														((Vector) p.get(i3)).get(columnPositionInTuple),
														((Vector) nonIndexedParameters.get(i2)).get(1),
														(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
												if (!compare) {
													matching = false;
												}

											}
											for (int i2 = 0; i2 < parameters.size(); i2++) {
												int columnPositionInTuple = t.columns
														.indexOf(((Vector) parameters.get(i2)).get(0));
												boolean compare = compareGeneral(
														((Vector) p.get(i3)).get(columnPositionInTuple),
														((Vector) parameters.get(i2)).get(1),
														(String) ((Vector) parameters.get(i2)).get(2));
												if (!compare) {
													matching = false;
												}
											}
											if (matching) {
												results.add(p.get(i3));
//												return results; //remove after test
											}

										}
									}
								}

							}

						}
					}

				}

				// a5erha

			}
			// Parameters contain el column elly ben sort bih
			else {

				// searchTuple.add(searchTerm);
				for (int i = 0; i < bucketNames.size(); i++) {
					if (index.buckets.contains((String) bucketNames.get(i))) {
						page b = (page) deserializeGeneral((String) bucketNames.get(i) + ".bucket");
						int startIndex = -1;

						// EQUAL OPERATOR
						if (operators.get(0).equals("=")) {
							startIndex = Collections.binarySearch(b, searchTuple);
//							Tuple equalTuple = (Tuple) b.get(startIndex);
//						for(int m = startIndex; m< )
							int m = startIndex;
							boolean duplicate = true; // fy duplicates
							while (duplicate == true && startIndex >= 0 && startIndex<b.size()) {
								boolean completeMatch = true;
//							if (((Vector) b.get(startIndex)).get(0) != searchTuple.get(0)) {
								if (!compareGeneral(((Vector) b.get(startIndex)).get(0), searchTuple.get(0),
										(String) operators.get(0))) {
									duplicate = false;
								} else {
									int counter = 0; // e7na fel search tuple 3and anhy index
									for (int l = 0; l < index.columnNames.size(); l++) {

										for (int h = 0; h < parameters.size(); h++) {
											if (((String) ((Vector) parameters.get(h)).get(0))
													.equalsIgnoreCase((String) index.columnNames.get(l))) {
													// changed b.get(StartIndex).get(H) TO L
												boolean compare = compareGeneral((Object) (((Vector) b.get(startIndex)).get(l)),(Object) searchTuple.get(counter),	(String) ((Vector) parameters.get(h)).get(2));
														
													
													
												if (compare == false) {
													completeMatch = false;
													startIndex++;
												} else {
													counter++;
													startIndex++; // added this
												}
											}
										}
									}
									// shelna el 7agat elly mesh bet match el index columns mel buckets
									if (completeMatch == true) {
										String pageNumber = (String) ((Vector) b.get(startIndex-1))
												.get(((Vector) b.get(startIndex-1)).size() - 1);

										if (!openedPages.contains(pageNumber)) {
											openedPages.add(pageNumber);

											page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
											if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
												// searchTuple vs b.get(startIndex)
												int row = Collections.binarySearch(p, b.get(startIndex-1));
												boolean matching = true;
												//
												for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
													int columnPositionInTuple = t.columns
															.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
													boolean compare = compareGeneral(
														
															((Vector) p.get(row)).get(columnPositionInTuple),
															((Vector) nonIndexedParameters.get(i2)).get(1),
															(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
													if (!compare) {
														matching = false;
													}

												}
												if (matching) {
													results.add(p.get(row));
												}
											} else { // CK mesh mawgoud

												for (int i3 = 0; i3 < p.size(); i3++) { // ck not present so we have to loop over the parameters too
														//change p.size()-2 to zero
													boolean matching = true;  //testing case
													for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
														int columnPositionInTuple = t.columns.indexOf(
																((Vector) nonIndexedParameters.get(i2)).get(0));
														boolean compare = compareGeneral(
																
																((Vector) p.get(i3)).get(columnPositionInTuple),
																((Vector) nonIndexedParameters.get(i2)).get(1),
																(String) ((Vector) nonIndexedParameters.get(i2))
																		.get(2));
														if (!compare) {
															matching = false;
														}
													}

													for (int i2 = 0; i2 < parameters.size(); i2++) {   //added this
														int columnPositionInTuple = t.columns
																.indexOf(((Vector) parameters.get(i2)).get(0));
														boolean compare = compareGeneral(
															
																((Vector) p.get(i3)).get(columnPositionInTuple),
																((Vector) parameters.get(i2)).get(1),
																(String) ((Vector) parameters.get(i2)).get(2));
														if (!compare) {
															matching = false;
														}
													}
													if (matching) {
														results.add(p.get(i3));
//														return results;  // remove after test

													}
												}
											}
										}
										else {
											continue;
										}

									}
								}
								
							}
						}
						// ay operator mesh EQUAL
						else {
							for (int i4 = 0; i4 < b.size(); i4++) {
								boolean completeMatch = true;
								for (int l = 0; l < index.columnNames.size(); l++) {
									for (int h = 0; h < parameters.size(); h++) {
										if (((String) ((Vector) parameters.get(h)).get(0))
												.equalsIgnoreCase((String) index.columnNames.get(l))) {
											boolean compare = compareGeneral((Object) (b.get(startIndex-1)),
													(Object) searchTuple.get((int) b.get(i4)),
													(String) ((Vector) parameters.get(h)).get(2));
											if (compare == false) {
												completeMatch = false;
											}
										}
									}
								}
								// shelna el 7agat elly mesh bet match el index columns mel buckets
								if (completeMatch) {
									String pageNumber = (String) ((Vector) b.get(startIndex-1))
											.get(((Vector) b.get(startIndex-1)).size() - 1);
									if (!openedPages.contains(pageNumber)) {
										openedPages.add(pageNumber);
										page p = (page) deserializeGeneral(tableName + "_" + pageNumber + ".page");
										if (t.columns.get(0).equals(index.columnNames.get(0))) { // CK mawgoud
											//searchTuple vs (b.get(startIndex-1)
											int row = Collections.binarySearch(p, (b.get(startIndex-1)));
											boolean matching = true;
											//
											for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
												int columnPositionInTuple = t.columns
														.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
												boolean compare = compareGeneral(
													
														((Vector) p.get(row)).get(columnPositionInTuple),
														((Vector) nonIndexedParameters.get(i2)).get(1),
														(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
												if (!compare) {
													matching = false;
												}

											}
											if (matching) {
												results.add(p.get(row));
											}
										} else { // CK mesh mawgoud
											boolean matching = true;

											for (int i3 = 0; i3 < p.size(); i3++) {
												for (int i2 = 0; i2 < nonIndexedParameters.size(); i2++) {
													int columnPositionInTuple = t.columns
															.indexOf(((Vector) nonIndexedParameters.get(i2)).get(0));
													boolean compare = compareGeneral(
															
															((Vector) p.get(i3)).get(columnPositionInTuple),
															((Vector) nonIndexedParameters.get(i2)).get(1),
															(String) ((Vector) nonIndexedParameters.get(i2)).get(2));
													if (!compare) {
														matching = false;
													}

												}
												if (matching) {
													results.add(p.get(i3));
												}
											}
										}
									}

								}

							}
						}
					}
				}
			}
		}
		return results;
	}

	private static boolean contains(String[] indexColumns, String object) {
		boolean result = false;
		for (String s : indexColumns) {
			if (s.equalsIgnoreCase(object)) {
				result = true;
				break;
			}
		}
		return result;
	}

	public static void insertions(DBApp dbApp) throws DBAppException {
		String strTableName = "student";
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");

		Hashtable colNameMin = new Hashtable();
		colNameMin.put("id", "0");
		colNameMin.put("name", "a");

		Hashtable colNameMax = new Hashtable();
		colNameMax.put("id", "10000");
		colNameMax.put("name", "ZZZZZZZ");

		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", 2);
		htblColNameValue.put("name", "Ahme");

		Hashtable htblColNameValue2 = new Hashtable();
		htblColNameValue2.put("id", 1);
		htblColNameValue2.put("name", "Mark");

		Hashtable htblColNameValue3 = new Hashtable();
		htblColNameValue3.put("id", 3);
		htblColNameValue3.put("name", "Yasmine");

		Hashtable htblColNameValue4 = new Hashtable();
		htblColNameValue4.put("id", 4);
		htblColNameValue4.put("name", "Carotte");

		Hashtable htblColNameValue5 = new Hashtable();
		htblColNameValue5.put("id", 9);
		htblColNameValue5.put("name", "Ninja");

		Hashtable htblColNameValue6 = new Hashtable();
		htblColNameValue6.put("id", 15);
		htblColNameValue6.put("name", "Croissant");

		Hashtable htblColNameValue66 = new Hashtable();
		htblColNameValue66.put("id", 20);
		htblColNameValue66.put("name", "pate accent");

		Hashtable htblColNameValue7 = new Hashtable();
		htblColNameValue7.put("id", 20);
		htblColNameValue7.put("name", "batates");

		Hashtable htblColNameValue8 = new Hashtable();
		htblColNameValue8.put("id", 10);
		htblColNameValue8.put("name", "gazar");

		Hashtable htblColNameValue9 = new Hashtable();
		htblColNameValue9.put("id", 400);
		htblColNameValue9.put("name", "ba7");

		Hashtable htblColNameValue10 = new Hashtable();
		htblColNameValue10.put("id", 5);
		htblColNameValue10.put("name", "YARaB");

		Hashtable htblColNameValue12 = new Hashtable();
		htblColNameValue12.put("id", 500);
		htblColNameValue12.put("name", "YARaB");

		Hashtable htblColNameValue13 = new Hashtable();
		htblColNameValue13.put("id", 550);
		htblColNameValue13.put("name", "YARaB");

		Hashtable htblColNameValuetest = new Hashtable();
		htblColNameValuetest.put("id", 22);
		htblColNameValuetest.put("name", "c");

//		dbApp.createTable(strTableName, "id", htblColNameType, colNameMin, colNameMax);
//		
		dbApp.insertIntoTable("student", htblColNameValue12);
//		dbApp.insertIntoTable("student", htblColNameValue13);
//		dbApp.insertIntoTable("student", htblColNameValuetest);
//
//		dbApp.insertIntoTable("student", htblColNameValue); // ahme
//		dbApp.insertIntoTable("student", htblColNameValue2); // mark error here
//
//		dbApp.insertIntoTable("student", htblColNameValue3);
//		dbApp.insertIntoTable("student", htblColNameValue4);
//		dbApp.insertIntoTable("student", htblColNameValue5);
//		dbApp.insertIntoTable("student", htblColNameValue6);
//		dbApp.insertIntoTable("student", htblColNameValue7);// back
//		dbApp.insertIntoTable("student", htblColNameValue8);
//		dbApp.insertIntoTable("student", htblColNameValue9);
//		dbApp.insertIntoTable("student", htblColNameValue10);

//		dbApp.insertIntoTable("student", htblColNameValue66);

	}

	public static void testTable(DBApp dbApp) throws DBAppException {
		Hashtable colNameType = new Hashtable<>();
		colNameType.put("dob", "java.util.Date");
		colNameType.put("id", "java.lang.Integer");
		colNameType.put("name", "java.lang.String");
		colNameType.put("gpa", "java.lang.Double");

		Hashtable colNameMin = new Hashtable<>();
		colNameMin.put("dob", "2000-01-01");
		colNameMin.put("id", "0");
		colNameMin.put("name", "aaaaa");
		colNameMin.put("gpa", "0.7");

		Hashtable colNameMax = new Hashtable<>();
		colNameMax.put("dob", "2001-01-01");
		colNameMax.put("id", "100");
		colNameMax.put("name", "zzzzz");
		colNameMax.put("gpa", "5");

		dbApp.createTable("test", "id", colNameType, colNameMin, colNameMax);
	}
	
	public static void testTableInsertions(DBApp dbApp) throws DBAppException {
		Hashtable htbl = new Hashtable();
	
		htbl.put("id", 0);
		htbl.put("name", "baaa"+"a");
		Date date = new Date(2000-1900,12-1,25);
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 1);
		htbl.put("name", "baaa"+"b");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 2);
		htbl.put("name", "baaa"+"c");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 3);
		htbl.put("name", "baaa"+"d");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 4);
		htbl.put("name", "baaa"+"e");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 5);
		htbl.put("name", "baaa"+"f");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 6);
		htbl.put("name", "baaa"+"g");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 7);
		htbl.put("name", "baaa"+"h");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		htbl.put("id", 8);
		htbl.put("name", "baaa"+"j");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
		
		dbApp.insertIntoTable("test", htbl);
		
		

	}
	

	public static void main(String[] args) throws IOException, DBAppException, ParseException {



//		try {
		DBApp dbApp = new DBApp();
		dbApp.init();

//			testTable(dbApp);
//		testTableInsertions(dbApp);
//		
//	
////		
//		String [] columnNames = {"id","name"};
//		dbApp.createIndex("test", columnNames);
		
		Date date = new Date(2000-1900,12-1,25);
		Hashtable htbl = new Hashtable();
		htbl.put("id", 16);
		htbl.put("name", "baaa"+"j");
		htbl.put("dob", date );
		htbl.put("gpa", 0.7);
//		dbApp.insertIntoTable("test", htbl);

		Hashtable htblUpdate = new Hashtable<>();
		htblUpdate.put("name", "carol" );
//		dbApp.updateTable("test", "20", htblUpdate);
		//20, 0.7, carol, Mon Dec 25 00:00:00 EET 2000
		
		Hashtable htblDelete = new Hashtable<>();
		htblDelete.put("name", "baaae" );
//		htblDelete.put("id", 15);
		htblDelete.put("dob", date );
		htblDelete.put("gpa", 0.7 );
		
		dbApp.deleteFromTable("test", htblDelete);
		
		try {
			Table t = deserialize("test");
			System.out.println("Table: " + t);
			for(int i =0;i<=4;i++) {
				page p = deserialize(i,"test");
				System.out.println("p"+i+ ": " + p);
			}
			
			System.out.println("--------");
			System.out.println("BUCKETS:");

		
			
//			Index indexTest = (Index) deserializeGeneral("Index0test.index");
//			
//			System.out.println(indexTest);
//			System.out.println("--------");
			page b1 = (page) deserializeGeneral("Index0test_3_1.bucket");
			
			page b2 = (page) deserializeGeneral("Index0test_1_1.bucket");
			page b3 = (page) deserializeGeneral("Index0test_2_1.bucket");
			
			page b4 = (page) deserializeGeneral("Index0test_3_4.bucket");
			System.out.println(b1);
			System.out.println("---------");
			System.out.println(b2);
			System.out.println("---------");
			
			System.out.println(b3);
			System.out.println("---------");
			
			System.out.println(b4);
			System.out.println("---------");
//			
//			

//			

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	

	}



}
