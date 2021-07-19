
public class OldCode {

//	public static void readFromCSV() {
//		String row = "";
//		BufferedReader csvReader;
//		try {
//			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
//			while ((row = csvReader.readLine()) != null) {
//				String[] data = row.split(",");
//				for (int i = 0; i < data.length; i++) {
//					System.out.print(data[i]);
//				}
//				System.out.println();
//			}
//			csvReader.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
//	public static boolean checkColumnExists(String tableName, String column) {
//		String row = "";
//		BufferedReader csvReader;
//		try {
//			csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
//			while ((row = csvReader.readLine()) != null) {
//				String[] data = row.split(",");
//				if (data[0].equalsIgnoreCase(tableName) && data[1].equalsIgnoreCase(column))
//					return true;
//
//			}
//			csvReader.close();
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return false;
//	}

//	public static void writeToCsv(String args) throws IOException {
//
//		FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
////    		String res="";
////    		for (int i=0; i<args.length; i++) {
////    			if (i==args.length-1)
////    				res+=args[i];
////    			else
////    				res+=args[i]+",";
////    		}
//
//		csvWriter.append(args);
//		csvWriter.append("\n");
//
//		csvWriter.flush();
//		csvWriter.close();
//	}
	 
		
		//old main
//		final DBApp dbApp = new DBApp();
//      dbApp.init();
//
//      BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
//      String record;
//      Hashtable<String, Object> row = new Hashtable<>();
//      int c = 0;
//      int finalLine = 1;
//      while ((record = coursesTable.readLine()) != null && c <= finalLine) {
//          if (c == finalLine) {
//              String[] fields = record.split(",");
//
//              int year = Integer.parseInt(fields[0].trim().substring(0, 4));
//              int month = Integer.parseInt(fields[0].trim().substring(5, 7));
//              int day = Integer.parseInt(fields[0].trim().substring(8));
//
//              Date dateAdded = new Date(year - 1900, month - 1, day);
//              row.put("date_added", dateAdded);
//              row.put("course_name", fields[2]);
//
//
//          }
//          c++;
//      }
//
//
//      String table = "courses";
//
//
////      dbApp.deleteFromTable(table, row);
//	   
//      try {
//			System.out.println(deserialize(table));
//			System.out.println(deserialize(0,table));
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
      
	         
//	       

	
//	public static void main(String[] args) {
//
//
//		
//
//		
//		
//		String strTableName = "student";
//		DBApp dbApp = new DBApp();
//		dbApp.init();
////		dbApp.insertions(dbApp);
//
////		Path source = Paths.get("src/main/resources/pages/newname");
////		Files.move(source, source.resolveSibling("student_9.page"));
//
//		try {
//
//		} catch (Exception e) {
//			System.out.println("batates");
//		}
//
////			
////			dbApp.insertIntoTable("student", htblColNameValue);
//
////			File f1 = new File("src/main/resources/pages/student_1.page");
////			File f2 = new File("src/main/resources/pages/student_10.page");
////			boolean b = f1.renameTo(f2);
////			System.out.println();
//
////			Path source = Paths.get("src/main/resources/pages/student_1.page");
////			Files.move(source, source.resolveSibling("student_10.page"));
////			System.out.println("---------------------------------");
//
////			INSERTION
////			Hashtable htblColNameValue = new Hashtable();
////			htblColNameValue.put("id", 11);
////			htblColNameValue.put("name", "a");
////			dbApp.insertIntoTable("student", htblColNameValue);	
//
//		// DELETION
//
////			System.out.println();
////			System.out.println("Search");
////			System.out.println(CKSearch(t, "student" , htblColNameValue));
////			System.out.println();
//
//		Table t;
//		try {
//
////			Hashtable htblColNameValue = new Hashtable();
////				htblColNameValue.put("id",455);
////			htblColNameValue.put("name", "a");
////				dbApp.insertIntoTable("student", htblColNameValue);	
////			dbApp.deleteFromTable("student", htblColNameValue);
//			
//			
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("name","MOATAZ");
//			dbApp.updateTable("student","480",htblColNameValue);
//			
//			t = deserialize("student");
//			System.out.println("main table");
//			System.out.println(t);
//			System.out.println();
//
////				
//
//			page p0 = deserialize(0, "student");
//			System.out.println("p0");
//			System.out.println(p0.toString());
//
//			page p00 = deserializeOverflow("0.0", "student");
//			System.out.println("p0.0");
//			System.out.println(p00.toString());
////				
//			page p01 = deserializeOverflow("0.1", "student");
//			System.out.println("p0.1");
//			System.out.println(p01.toString());
////				
////				page p02 = deserializeOverflow("0.2", "student");
////				System.out.println("p0.2");
////				System.out.println(p02.toString());
//
//			page p1 = deserialize(1, "student");
//			System.out.println("page1");
//			System.out.println(p1.toString());
//
////			
//////				
//			page p10 = deserializeOverflow("1.0", "student");
//			System.out.println("p1.0");
//			System.out.println(p10.toString());
//
//			page p11 = deserializeOverflow("1.1", "student");
//			System.out.println("p1.1");
//			System.out.println(p11.toString());
//			//
////			page p2 = deserialize(2, "student");
////			System.out.println("page2");
////			System.out.println(p2.toString());
//			//
//
////				
////			
//			//
//
//			//
////				
//			//
//			//
//			//
//
////				
////			
//			//
////				page p3 = deserialize(3, "student");
////				System.out.println("page3");
////				System.out.println(p3.toString());
//
////				page p4 = deserializeOverflow("2.0", "student");
////				System.out.println("page2.0");
////				System.out.println(p4.toString());
//
////				page p5 = deserializeOverflow("2.1", "student");
////				System.out.println("page2.1");
////				System.out.println(p5.v.toString());
////				System.out.println(p5.size());
//			//
////				page p6 = deserializeOverflow("2.2", "student");
////				System.out.println("page2.2");
////				System.out.println(p6.v.toString());
////				System.out.println(p6.size());
//			//
////				page p7 = deserializeOverflow("2.3", "student");
////				System.out.println("page2.3");
////				System.out.println(p7.v.toString());
////				System.out.println(p7.size());
//			//
////				page p8 = deserializeOverflow("2.4", "student");
////				System.out.println("page2.4");
////				System.out.println(p8.v.toString());
////				System.out.println(p8.size());
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//
//	}
//	}
	
}

	

