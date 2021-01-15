package tests;
//originally from : joins.C

import iterator.*;
import iterator.Iterator;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
*/

//Define the Sailor schema
class Sailor {
  public int    sid;
  public String sname;
  public int    rating;
  public double age;
  
  public Sailor (int _sid, String _sname, int _rating,double _age) {
    sid    = _sid;
    sname  = _sname;
    rating = _rating;
    age    = _age;
  }
}

//Define the Boat schema
class Boats {
  public int    bid;
  public String bname;
  public String color;
  
  public Boats (int _bid, String _bname, String _color) {
    bid   = _bid;
    bname = _bname;
    color = _color;
  }
}

//Define the Reserves schema
class Reserves {
  public int    sid;
  public int    bid;
  public String date;
  
  public Reserves (int _sid, int _bid, String _date) {
    sid  = _sid;
    bid  = _bid;
    date = _date;
  }
}

class JoinsDriver implements GlobalConst {
	
	private static final String path_to_queries = "/Users/hugodanet/Downloads/DBSYS IEJoin/queriesdata/";
	
	private FileWriter fileWriter;
    private PrintWriter printWriter;
  
  private boolean OK = true;
  private boolean FAIL = false;
  private Vector sailors;
  private Vector boats;
  private Vector reserves;
  /** Constructor
   */
  public JoinsDriver() {
    
    //build Sailor, Boats, Reserves table
    sailors  = new Vector();
    boats    = new Vector();
    reserves = new Vector();
    
    sailors.addElement(new Sailor(53, "Bob Holloway",       9, 53.6));
    sailors.addElement(new Sailor(54, "Susan Horowitz",     1, 34.2));
    sailors.addElement(new Sailor(57, "Yannis Ioannidis",   8, 40.2));
    sailors.addElement(new Sailor(59, "Deborah Joseph",    10, 39.8));
    sailors.addElement(new Sailor(61, "Landwebber",         8, 56.7));
    sailors.addElement(new Sailor(63, "James Larus",        9, 30.3));
    sailors.addElement(new Sailor(64, "Barton Miller",      5, 43.7));
    sailors.addElement(new Sailor(67, "David Parter",       1, 99.9));   
    sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.addElement(new Sailor(71, "Guri Sohi",         10, 42.1));
    sailors.addElement(new Sailor(73, "Prasoon Tiwari",     8, 39.2));
    sailors.addElement(new Sailor(39, "Anne Condon",        3, 30.3));
    sailors.addElement(new Sailor(47, "Charles Fischer",    6, 46.3));
    sailors.addElement(new Sailor(49, "James Goodman",      4, 50.3));
    sailors.addElement(new Sailor(50, "Mark Hill",          5, 35.2));
    sailors.addElement(new Sailor(75, "Mary Vernon",        7, 43.1));
    sailors.addElement(new Sailor(79, "David Wood",         3, 39.2));
    sailors.addElement(new Sailor(84, "Mark Smucker",       9, 25.3));
    sailors.addElement(new Sailor(87, "Martin Reames",     10, 24.1));
    sailors.addElement(new Sailor(10, "Mike Carey",         9, 40.3));
    sailors.addElement(new Sailor(21, "David Dewitt",      10, 47.2));
    sailors.addElement(new Sailor(29, "Tom Reps",           7, 39.1));
    sailors.addElement(new Sailor(31, "Jeff Naughton",      5, 35.0));
    sailors.addElement(new Sailor(35, "Miron Livny",        7, 37.6));
    sailors.addElement(new Sailor(37, "Marv Solomon",      10, 48.9));

    boats.addElement(new Boats(1, "Onion",      "white"));
    boats.addElement(new Boats(2, "Buckey",     "red"  ));
    boats.addElement(new Boats(3, "Enterprise", "blue" ));
    boats.addElement(new Boats(4, "Voyager",    "green"));
    boats.addElement(new Boats(5, "Wisconsin",  "red"  ));
 
    reserves.addElement(new Reserves(10, 1, "05/10/95"));
    reserves.addElement(new Reserves(21, 1, "05/11/95"));
    reserves.addElement(new Reserves(10, 2, "05/11/95"));
    reserves.addElement(new Reserves(31, 1, "05/12/95"));
    reserves.addElement(new Reserves(10, 3, "05/13/95"));
    reserves.addElement(new Reserves(69, 4, "05/12/95"));
    reserves.addElement(new Reserves(69, 5, "05/14/95"));
    reserves.addElement(new Reserves(21, 5, "05/16/95"));
    reserves.addElement(new Reserves(57, 2, "05/10/95"));
    reserves.addElement(new Reserves(35, 3, "05/15/95"));

    boolean status = OK;
    int numsailors = 25;
    int numsailors_attrs = 4;
    int numreserves = 10;
    int numreserves_attrs = 3;
    int numboats = 5;
    int numboats_attrs = 3;
    
    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }

   
    /*
    ExtendedSystemDefs extSysDef = 
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
    
    // creating the sailors relation
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

    //SOS
    short [] Ssizes = new short [1];
    Ssizes[0] = 30; //first elt. is 30
    
    Tuple t = new Tuple();
    try {
      t.setHdr((short) 4,Stypes, Ssizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    int size = t.size();
    
    // inserting the tuple into file "sailors"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numsailors; i++) {
      try {
	t.setIntFld(1, ((Sailor)sailors.elementAt(i)).sid);
	t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
	t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
	t.setFloFld(4, (float)((Sailor)sailors.elementAt(i)).age);
      }
      catch (Exception e) {
	System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the boats relation
    AttrType [] Btypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrString), 
    };
    
    short  []  Bsizes = new short[2];
    Bsizes[0] = 30;
    Bsizes[1] = 20;
    t = new Tuple();
    try {
      t.setHdr((short) 3,Btypes, Bsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file "boats"
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("boats.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 3, Btypes, Bsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numboats; i++) {
      try {
	t.setIntFld(1, ((Boats)boats.elementAt(i)).bid);
	t.setStrFld(2, ((Boats)boats.elementAt(i)).bname);
	t.setStrFld(3, ((Boats)boats.elementAt(i)).color);
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for boats");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the boats relation
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrString);

    short [] Rsizes = new short [1];
    Rsizes[0] = 15; 
    t = new Tuple();
    try {
      t.setHdr((short) 3,Rtypes, Rsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file "boats"
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("reserves.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 3, Rtypes, Rsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numreserves; i++) {
      try {
	t.setIntFld(1, ((Reserves)reserves.elementAt(i)).sid);
	t.setIntFld(2, ((Reserves)reserves.elementAt(i)).bid);
	t.setStrFld(3, ((Reserves)reserves.elementAt(i)).date);

      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }      
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for reserves");
      Runtime.getRuntime().exit(1);
    }
    
  }
  
  private boolean File2Heap(String fileNameInput, String fileNameOutput, int max_num_tuples){
	     
	    /**
	    * 
	    * BUILD table from "fileInput"
	    * 
	    *  @parameter fileNameInput Name of file containing data
	    *  @parameter fileNameOutput Name of table saved in the DB
	    *  @parameter max_num_tuples Max number of tuple to load from the file in case ofbig files. 
	    *  
	    * **/  
	   
	    if(fileNameInput==null || fileNameOutput==null) {
	     return false;
	    }
	    
	    if(max_num_tuples<=0) {
	     max_num_tuples=Integer.MAX_VALUE; // Load tuples until the HeapFile can contain them
	    }
	     /* Create relation */
	     
	     AttrType [] types = new AttrType[4];
	     types[0] = new AttrType (AttrType.attrInteger);
	     types[1] = new AttrType (AttrType.attrInteger);
	     types[2] = new AttrType (AttrType.attrInteger);
	     types[3] = new AttrType (AttrType.attrInteger);
	     
	     short numField=4;
	       
	     Tuple t = new Tuple();
	       
	     try {
	      t.setHdr(numField,types, null);
	     }
	     catch (Exception e) {
	       
	      System.err.println("*** error in Tuple.setHdr() ***");
	         e.printStackTrace();
	         return false;
	     }
	       
	      int t_size = t.size();
	       
	      RID rid;
	       
	      Heapfile f = null;
	       
	      try {
	       f = new Heapfile(fileNameOutput);
	      }
	       
	      catch (Exception e) {
	       System.err.println("*** error in Heapfile constructor ***");
	       e.printStackTrace();
	       return false;
	      }
	       
	       
	      t = new Tuple(t_size);
	      
	      try {
	       t.setHdr((short) 4, types, null);
	      }
	      catch (Exception e) {
	       System.err.println("*** error in Tuple.setHdr() ***");
	       e.printStackTrace();
	       return false;
	      }
	      
	      int cont=0; // To limit the size of table
	      
	      try {
	    
	       File file = new File(fileNameInput);
	       BufferedReader reader=null;
	       reader = new BufferedReader(new FileReader(file));
	    
	       String text = null;
	       text = reader.readLine(); //To skip header
	       text="";
	       
	       while ((text = reader.readLine()) != null && cont!=max_num_tuples) {
	         
	        String[] attributes=text.split(",");
	        t.setIntFld(1, Integer.parseInt(attributes[0]));
	        t.setIntFld(2, Integer.parseInt(attributes[1]));    
	        t.setIntFld(3, Integer.parseInt(attributes[2]));
	        t.setIntFld(4, Integer.parseInt(attributes[3]));
	        f.insertRecord(t.getTupleByteArray());
	        cont++;
	    }
	    reader.close();
	      }
	      catch(FileNotFoundException e1) {
	       System.err.println("*** File "+fileNameInput+" ***");
	       e1.printStackTrace();
	       return false;
	      }
	      catch (Exception e) {
	       
	       System.err.println("*** Heapfile error in Tuple.setIntFld() ***");
	       e.printStackTrace();
	       return false;
	        
	      }   
	      
	      System.out.println("Number of tuple inserted: "+cont);  
	      return true;
	      
	}

private  String[][] File2List(String myfile)  {
		
	BufferedReader abc;
	try {
		abc = new BufferedReader(new FileReader(myfile));
	
	int M=(int) abc.lines().count();
	abc.close();
	
	int i=0;
	
	abc = new BufferedReader(new FileReader(myfile));
	String [][] east ;
	String line;
	line = abc.readLine() + ","+ Integer.toString(0);

	String[] dimes = line.split(",");
	
	int N=dimes.length;
	
	east= new String [M][N];
	 
	east[0]=dimes;
	 
	
	for( i=1;i<M;i++) {
	   line = abc.readLine();
	   dimes = line.split(",");
	   east[i]=dimes;	 
	                   }
	 abc.close();
	
	return east;}
 catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	}
	 
	}

//this function returns the different columns to join from a txt query file
private ArrayList<Integer> GetColumns(String query_file_name){
	ArrayList<Integer> columns = new ArrayList<Integer>();
    Scanner scanner;
    ArrayList<String> lines = null;
	try {
		scanner = new Scanner(new File(query_file_name));
	
	lines = new ArrayList<String>();


	while (scanner.hasNextLine()) {
	  String line = scanner.nextLine();
	  lines.add(line);
	}
		
	String[] words = lines.get(0).split(" ");
	String[] words2 = lines.get(2).split(" ");
	
	columns.add(Character.getNumericValue(words[0].charAt(2)));
	columns.add(Character.getNumericValue(words[1].charAt(2)));
	
	columns.add(Character.getNumericValue(words2[0].charAt(2)));
	columns.add(Character.getNumericValue(words2[2].charAt(2)));
	
	
	if(lines.size() > 3) {
		String[] words3 = lines.get(4).split(" ");
		columns.add(Character.getNumericValue(words3[0].charAt(2)));
		columns.add(Character.getNumericValue(words3[2].charAt(2)));
	}
		
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	}
	
	return columns;
}

//this function returns the name of the tables in the txt query file
private ArrayList<Character> GetTables(String query_file_name){
	ArrayList<Character> columns = new ArrayList<Character>();
    Scanner scanner;
    ArrayList<String> lines = null;
	try {
		scanner = new Scanner(new File(query_file_name));
	
	lines = new ArrayList<String>();

	while (scanner.hasNextLine()) {
	  String line = scanner.nextLine();
	  lines.add(line);
	}
		
	String[] words = lines.get(2).split(" ");
	
	columns.add(words[0].charAt(0));
	columns.add(words[2].charAt(0));
		
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	}
	return columns;
}


//This function writes a single predicate conditional expression with a txt query file as an input
private void File2SingleCondExpr(CondExpr[] expr, String query_file_name) {
	
    Scanner scanner;
	try {
		scanner = new Scanner(new File(query_file_name));
	
	ArrayList<String> lines = new ArrayList<String>();
	
	while (scanner.hasNextLine()) {
	  String line = scanner.nextLine();
	  lines.add(line);
	}
		
	String[] words1 = lines.get(0).split(" ");
	String[] words2 = lines.get(1).split(" ");
	String[] words3 = lines.get(2).split(" ");

	expr[0].next  = null;
	expr[0].op    = new AttrOperator(Integer.parseInt(words3[1]));
	expr[0].type1 = new AttrType(AttrType.attrSymbol);
	expr[0].type2 = new AttrType(AttrType.attrSymbol);
	expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),Character.getNumericValue(words3[0].charAt(2)));
	expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Character.getNumericValue(words3[2].charAt(2)));

	expr[1] = null;
	
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

//This function writes a double predicates conditional expression with a txt query file as an input
private void File2DoubleCondExpr(CondExpr[] expr, String query_file_name) {

    Scanner scanner;
	try {
		scanner = new Scanner(new File(query_file_name));
	
	ArrayList<String> lines = new ArrayList<String>();
	while (scanner.hasNextLine()) {
		  String line = scanner.nextLine();
		  lines.add(line);
		}
	
	String[] words1 = lines.get(0).split(" ");
	String[] words2 = lines.get(1).split(" ");
	String[] words3 = lines.get(2).split(" ");
	String[] words4 = lines.get(3).split(" ");
	String[] words5 = lines.get(4).split(" ");

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(Integer.parseInt(words3[1]));
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Character.getNumericValue(words3[0].charAt(2)));
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Character.getNumericValue(words3[2].charAt(2)));


    expr[1].next  = null;
    expr[1].op    = new AttrOperator(Integer.parseInt(words5[1]));
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrSymbol);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Character.getNumericValue(words5[0].charAt(2)));
    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Character.getNumericValue(words5[0].charAt(2)));
 
    expr[2] = null;
    
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
  
  public boolean runTests(int nb_of_tuples) {
    
    Disclaimer();
    LoadDB(nb_of_tuples);
    
    try {
		fileWriter = new FileWriter(path_to_queries + "output.txt");
		printWriter = new PrintWriter(fileWriter);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
    //Query1();
    
    //Query2();
    //Query3();
    
   
    //Query4();
    //Query5();
    //Query6();
    
    //Query7();
    //Query8();
    
    
    //Query1a(path_to_queries + "/query_1a.txt");
    //Query1b(path_to_queries + "/query_1b.txt");
        
    //Query2a(path_to_queries + "/query_2a.txt");
    //Query2aOptim(path_to_queries + "/query_2a.txt");
    
    //Query2b(path_to_queries + "/query_2b.txt");
    //Query2bOptim(path_to_queries + "/query_2b.txt");

    //Query2c(path_to_queries + "/query_2c.txt");
    //Query2c(path_to_queries + "/query_2c_1.txt");
    //Query2c(path_to_queries + "/query_2c_2.txt");
    
    printWriter.close();

    return true;
  }

  private void Query1_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[1].next  = null;
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
    expr[1].operand2.integer = 2;
 
    expr[2] = null;
  }

  private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
	    
	    expr[1] = null;
	 
	    expr2[0].next  = null;
	    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
	    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr2[0].type2 = new AttrType(AttrType.attrSymbol);   
	    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
	    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
	    
	    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
	    expr2[1].next = null;
	    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr2[1].type2 = new AttrType(AttrType.attrString);
	    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
	    expr2[1].operand2.string = "red";
	 
	    expr2[2] = null;
	  }

  private void Query3_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    expr[1] = null;
  }

  private CondExpr[] Query5_CondExpr() {
    CondExpr [] expr2 = new CondExpr[3];
    expr2[0] = new CondExpr();
    
   
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
    
    expr2[1] = new CondExpr();
    expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
    expr2[1].next = null;
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
   
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
    expr2[1].type2 = new AttrType(AttrType.attrReal);
    expr2[1].operand2.real = (float)40.0;
    

    expr2[1].next = new CondExpr();
    expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
    expr2[1].next.next = null;
    expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
    expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
    expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
    expr2[1].next.operand2.integer = 7;
 
    expr2[2] = null;
    return expr2;
  }

  private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
   
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr[1].next  = null;
    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand2.integer = 7;
 
    expr[2] = null;
 
    expr2[0].next  = null;
    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
    
    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

    expr2[1].next = null;
    expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);
    
    expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    expr2[1].type2 = new AttrType(AttrType.attrString);
    expr2[1].operand2.string = "red";
 
    expr2[2] = null;
  }
  
  
  private void Query7_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

	  	//sid = sid
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

	    //s.age > 35
	    expr[1].next  = null;
	    expr[1].op    = new AttrOperator(AttrOperator.aopGE);
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
	    expr[1].type2 = new AttrType(AttrType.attrReal);
	    expr[1].operand2.real = (float) 40.3;
	 
	    //null
	    expr[2] = null;
	 
	    //bid = bid
	    expr2[0].next  = null;
	    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
	    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
	 
	    //null
	    expr2[1] = null;
	  }
  
  private void Query8_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

	  	//sid = sid
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

	    //s.age >= 40
	    expr[1].next  = null;
	    expr[1].op    = new AttrOperator(AttrOperator.aopGE);
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
	    expr[1].type2 = new AttrType(AttrType.attrReal);
	    expr[1].operand2.real = (float) 40;
	    
	    //s.age <= 45
	    expr[2].next  = null;
	    expr[2].op    = new AttrOperator(AttrOperator.aopLE);
	    expr[2].type1 = new AttrType(AttrType.attrSymbol);
	    expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
	    expr[2].type2 = new AttrType(AttrType.attrReal);
	    expr[2].operand2.real = (float) 45;
	 
	    //null
	    expr[3] = null;
	 
	    //bid = bid
	    expr2[0].next  = null;
	    expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    expr2[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
	    expr2[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
	 
	    //null
	    expr2[1] = null;
	  }

  public void Query1() {
    
    System.out.print("**********************Query1 starting *********************\n");
    boolean status = OK;
    
    // Sailors, Boats, Reserves Queries.
    System.out.print ("Query: Find the names of sailors who have reserved "
		      + "boat number 2.\n"
		      + "       and print out the date of reservation.\n\n"
		      + "  SELECT S.sname, R.date\n"
		      + "  FROM   Sailors S, Reserves R\n"
		      + "  WHERE  S.sid = R.sid AND R.bid = 2\n\n");
    
    System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");
 
    CondExpr[] outFilter = new CondExpr[3];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
    outFilter[2] = new CondExpr();
 
    Query1_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30
    
    FldSpec [] Sprojection = new FldSpec[4];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    CondExpr [] selects = new CondExpr [1];
    selects = null;
    
 
    FileScan am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    AttrType [] Rtypes = new AttrType[3];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrString);

    short [] Rsizes = new short[1];
    Rsizes[0] = 15; 
    FldSpec [] Rprojection = new FldSpec[3];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
 
    FileScan am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short) 3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
   
    
    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

    AttrType [] jtype = new AttrType[2];
    jtype[0] = new AttrType (AttrType.attrString);
    jtype[1] = new AttrType (AttrType.attrString);
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4, 
			 1, 4, 
			 10,
			 am, am2, 
			 false, false, ascending,
			 outFilter, proj_list, 2);
    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***"); 
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

   
 
    QueryCheck qcheck1 = new QueryCheck(1);
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);

        qcheck1.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }
    
    qcheck1.report(1);
    
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
  }
  
  public void Query2() {
    System.out.print("**********************Query2 starting *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print 
      ("Query: Find the names of sailors who have reserved "
       + "a red boat\n"
       + "       and return them in alphabetical order.\n\n"
       + "  SELECT   S.sname\n"
       + "  FROM     Sailors S, Boats B, Reserves R\n"
       + "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
       + "  ORDER BY S.sname\n"
       + "Plan used:\n"
       + " Sort (Pi(sname) (Sigma(B.color='red')  "
       + "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
       + "(Tests File scan, Index scan ,Projection,  index selection,\n "
       + "sort and simple nested-loop join.)\n\n");
    
    // Build Index first
    IndexType b_index = new IndexType (IndexType.B_Index);

   
    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }
    
    


    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    CondExpr [] outFilter2 = new CondExpr[3];
    outFilter2[0] = new CondExpr();
    outFilter2[1] = new CondExpr();
    outFilter2[2] = new CondExpr();

    Query2_CondExpr(outFilter, outFilter2);
    Tuple t = new Tuple();
    t = null;

    AttrType [] Stypes = {
      new AttrType(AttrType.attrInteger), //sid
      new AttrType(AttrType.attrString), //sname
      new AttrType(AttrType.attrInteger), //rating
      new AttrType(AttrType.attrReal) //age
    };

    AttrType [] Stypes2 = {
      new AttrType(AttrType.attrInteger), //bid
      new AttrType(AttrType.attrString), //sname
    };

    short []   Ssizes = new short[1];
    Ssizes[0] = 30;
    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger), //sid
      new AttrType(AttrType.attrInteger), //bid
      new AttrType(AttrType.attrString), //date
    };

    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 15;
    AttrType [] Btypes = {
      new AttrType(AttrType.attrInteger), //bid
      new AttrType(AttrType.attrString), //bname
      new AttrType(AttrType.attrString), //color
    };

    short  []  Bsizes = new short[2];
    Bsizes[0] =30;
    Bsizes[1] =20;
    AttrType [] Jtypes = {
      new AttrType(AttrType.attrString), //sname
      new AttrType(AttrType.attrInteger), //bid
    };

    short  []  Jsizes = new short[1]; //after pi(sname)
    Jsizes[0] = 30;
    AttrType [] JJtype = {
      new AttrType(AttrType.attrString), //sname
    };

    short [] JJsize = new short[1]; //after pi(sname,bid)
    JJsize[0] = 30;
    FldSpec []  proj1 = {
       new FldSpec(new RelSpec(RelSpec.outer), 2), //S.sname
       new FldSpec(new RelSpec(RelSpec.innerRel), 2) //R.bid
    }; // S.sname, R.bid

    FldSpec [] proj2  = {
       new FldSpec(new RelSpec(RelSpec.outer), 1) //sname
    };
 
    FldSpec [] Sprojection = { // ?
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
 
    CondExpr [] selects = new CondExpr[1];
    selects[0] = null;
    
    
    //IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am = null;
   

    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
        Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID rid = new RID();
    int key =0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);
      
      try {
	key = tt.getIntFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    
    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print ("After Building btree index on sailors.sid.\n\n");
    try {
      am = new IndexScan ( b_index, "sailors.in",
			   "BTreeIndex", Stypes, Ssizes, 4, 2,
			   Sprojection, null, 1, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Stypes2, 2, Ssizes,
				  Rtypes, 3, Rsizes,
				  10,
				  am, "reserves.in",
				  outFilter, null, proj1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

     NestedLoopsJoins nlj2 = null ; 
    try {
      nlj2 = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				   Btypes, 3, Bsizes,
				   10,
				   nlj, "boats.in",
				   outFilter2, null, proj2, 1);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;
    try {
      sort_names = new Sort (JJtype,(short)1, JJsize,
			     (iterator.Iterator) nlj2, 1, ascending, JJsize[0], 10);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    
    QueryCheck qcheck2 = new QueryCheck(2);
    
   
    t = null;
    try {
      while ((t = sort_names.get_next()) != null) {
        t.print(JJtype);
        qcheck2.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    qcheck2.report(2);

    System.out.println ("\n"); 
    try {
      sort_names.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);
      }
  }
  

   public void Query3() {
    System.out.print("**********************Query3 starting *********************\n"); 
    boolean status = OK;

        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ( "Query: Find the names of sailors who have reserved a boat.\n\n"
	+ "  SELECT S.sname\n"
	+ "  FROM   Sailors S, Reserves R\n"
	+ "  WHERE  S.sid = R.sid\n\n"
	+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");
    
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck3 = new QueryCheck(3);
 
   
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck3.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
       Runtime.getRuntime().exit(1);
    }
 
 
    qcheck3.report(3);
 
    System.out.println ("\n"); 
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
  }

   public void Query4() {
     System.out.print("**********************Query4 starting *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of sailors who have reserved a boat\n"
       + "       and print each name once.\n\n"
       + "  SELECT DISTINCT S.sname\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid\n\n"
       + "(Tests FileScan, Projection, Sort-Merge Join and "
       + "Duplication elimination.)\n\n");
 
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
 
    Query3_CondExpr(outFilter);
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;
 
    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes,
				  (short)4, (short) 4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3)
    }; 
 
    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
				  (short)3, (short)3,
				  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrString) };
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    short  []  jsizes    = new short[1];
    jsizes[0] = 30;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 1);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }
    
   

    DuplElim ed = null;
    try {
      ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
 
    QueryCheck qcheck4 = new QueryCheck(4);

    
    t = null;
 
    try {
      while ((t = ed.get_next()) != null) {
        t.print(jtype);
        qcheck4.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace(); 
      Runtime.getRuntime().exit(1);
      }
    
    qcheck4.report(4);
    try {
      ed.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
   System.out.println ("\n");  
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
 }

   public void Query5() {
   System.out.print("**********************Query5 starting *********************\n");  
    boolean status = OK;
        // Sailors, Boats, Reserves Queries.
 
    System.out.print 
      ("Query: Find the names of old sailors or sailors with "
       + "a rating less\n       than 7, who have reserved a boat, "
       + "(perhaps to increase the\n       amount they have to "
       + "pay to make a reservation).\n\n"
       + "  SELECT S.sname, S.rating, S.age\n"
       + "  FROM   Sailors S, Reserves R\n"
       + "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
       + "(Tests FileScan, Multiple Selection, Projection, "
       + "and Sort-Merge Join.)\n\n");

   
    CondExpr [] outFilter;
    outFilter = Query5_CondExpr();
 
    Tuple t = new Tuple();
    t = null;
 
    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrReal)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrString),
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] = 15;

    FldSpec [] Sprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
    
    CondExpr[] selects = new CondExpr [1];
    selects[0] = null;
 
    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3),
      new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    FldSpec [] Rprojection = {
      new FldSpec(new RelSpec(RelSpec.outer), 1),
      new FldSpec(new RelSpec(RelSpec.outer), 2),
      new FldSpec(new RelSpec(RelSpec.outer), 3)
    };
  
    AttrType [] jtype     = { 
      new AttrType(AttrType.attrString), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrReal)
    };


    iterator.Iterator am = null;
    try {
      am  = new FileScan("sailors.in", Stypes, Ssizes, 
				  (short)4, (short)4,
				  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
    
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
			 (short)3, (short)3,
			 Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }
 
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
			 Rtypes, 3, Rsizes,
			 1, 4,
			 1, 4,
			 10,
			 am, am2,
			 false, false, ascending,
			 outFilter, proj_list, 3);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }
 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

    QueryCheck qcheck5 = new QueryCheck(5);
    //Tuple t = new Tuple();
    t = null;
 
    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
        qcheck5.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    qcheck5.report(5);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n"); 
    if (status != OK) {
      //bail out
      System.err.println ("*** Error close for sortmerge");
      Runtime.getRuntime().exit(1);
    }
 }

   public void Query6()
   {
     System.out.print("**********************Query6 starting *********************\n");
     boolean status = OK;
     // Sailors, Boats, Reserves Queries.
     System.out.print( "Query: Find the names of sailors with a rating greater than 7\n"
			+ "  who have reserved a red boat, and print them out in sorted order.\n\n"
			+ "  SELECT   S.sname\n"
			+ "  FROM     Sailors S, Boats B, Reserves R\n"
			+ "  WHERE    S.sid = R.sid AND S.rating > 7 AND R.bid = B.bid \n"
			+ "           AND B.color = 'red'\n"
			+ "  ORDER BY S.name\n\n"
			
			+ "Plan used:\n"
			+" Sort(Pi(sname) (Sigma(B.color='red')  |><|  Pi(sname, bid) (Sigma(S.rating > 7)  |><|  R)))\n\n"
			
			+ "(Tests FileScan, Multiple Selection, Projection,sort and nested-loop join.)\n\n");
     
     CondExpr [] outFilter  = new CondExpr[3];
     outFilter[0] = new CondExpr();
     outFilter[1] = new CondExpr();
     outFilter[2] = new CondExpr();
     CondExpr [] outFilter2 = new CondExpr[3];
     outFilter2[0] = new CondExpr();
     outFilter2[1] = new CondExpr();
     outFilter2[2] = new CondExpr();
     
     Query6_CondExpr(outFilter, outFilter2);
     Tuple t = new Tuple();
     t = null;
     
     AttrType [] Stypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrReal)
     };
     
     
     
     short []   Ssizes = new short[1];
     Ssizes[0] = 30;
     AttrType [] Rtypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
     };
     
     short  []  Rsizes = new short[1] ;
     Rsizes[0] = 15;
     AttrType [] Btypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrString), 
     };
     
     short  []  Bsizes = new short[2];
     Bsizes[0] =30;
     Bsizes[1] =20;
     
     
     AttrType [] Jtypes = {
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
     };
     
     short  []  Jsizes = new short[1];
     Jsizes[0] = 30;
     AttrType [] JJtype = {
	new AttrType(AttrType.attrString), 
     };
     
     short [] JJsize = new short[1];
     JJsize[0] = 30; 
     
     
     
     FldSpec []  proj1 = {
	new FldSpec(new RelSpec(RelSpec.outer), 2),
	new FldSpec(new RelSpec(RelSpec.innerRel), 2)
     }; // S.sname, R.bid
     
     FldSpec [] proj2  = {
	new FldSpec(new RelSpec(RelSpec.outer), 1)
     };
     
     FldSpec [] Sprojection = {
	new FldSpec(new RelSpec(RelSpec.outer), 1),
	new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
     };
     
     
     
     
     
     FileScan am = null;
     try {
	am  = new FileScan("sailors.in", Stypes, Ssizes, 
			   (short)4, (short)4,
			   Sprojection, null);
     }
     catch (Exception e) {
	status = FAIL;
	System.err.println (""+e);
	e.printStackTrace();
     }
     
     if (status != OK) {
	//bail out
	
	System.err.println ("*** Error setting up scan for sailors");
	Runtime.getRuntime().exit(1);
     }
     
 
     
     NestedLoopsJoins inl = null;
     try {
	inl = new NestedLoopsJoins (Stypes, 4, Ssizes,
				    Rtypes, 3, Rsizes,
				    10,
				  am, "reserves.in",
				    outFilter, null, proj1, 2);
     }
     catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
     }
    
     System.out.print( "After nested loop join S.sid|><|R.sid.\n");
	
     NestedLoopsJoins nlj = null;
     try {
	nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				    Btypes, 3, Bsizes,
				    10,
				    inl, "boats.in",
				    outFilter2, null, proj2, 1);
     }
     catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
     }
     
     System.out.print( "After nested loop join R.bid|><|B.bid AND B.color=red.\n");
     
     TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
     Sort sort_names = null;
     try {
	sort_names = new Sort (JJtype,(short)1, JJsize,
			       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
     }
     catch (Exception e) {
	System.err.println ("*** Error preparing for sorting");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
     }
     
     
     System.out.print( "After sorting the output tuples.\n");
  
     
     QueryCheck qcheck6 = new QueryCheck(6);
     
     try {
	while ((t =sort_names.get_next()) !=null) {
	  t.print(JJtype);
	  qcheck6.Check(t);
	}
     }catch (Exception e) {
	System.err.println ("*** Error preparing for get_next tuple");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
     }
     
     qcheck6.report(6);
     
     System.out.println ("\n"); 
     try {
	sort_names.close();
     }
     catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
     }
     
     if (status != OK) {
	//bail out
	
	Runtime.getRuntime().exit(1);
     }
     
   }
   

   public void Query7() {
	   
	     System.out.print("**********************Query7 starting *********************\n");
	     boolean status = OK;
	     // Sailors, Boats, Reserves Queries.
	     System.out.print(""
				+ "  SELECT   B.bname\n"
				+ "  FROM     Sailors S, Boats B, Reserves R\n"
				+ "  WHERE    S.sid = R.sid AND R.bid = B.bid AND S.age >= 35 \n"
				+ "  ORDER BY B.bname\n\n"
				
				+ "Plan used:\n"
				+" Sort(Pi(bname) (B  |><|  Pi(sname, bid) (Sigma(S.age>=35)  |><|  R)))\n\n");
	     
	     CondExpr [] outFilter  = new CondExpr[3];
	     outFilter[0] = new CondExpr();
	     outFilter[1] = new CondExpr();
	     outFilter[2] = new CondExpr();
	     CondExpr [] outFilter2 = new CondExpr[2];
	     outFilter2[0] = new CondExpr();
	     outFilter2[1] = new CondExpr();
	     
	     Query7_CondExpr(outFilter, outFilter2);
	     Tuple t = new Tuple();
	     t = null;
	     
	     AttrType [] Stypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrReal)
	     };
	     
	     
	     
	     short []   Ssizes = new short[1];
	     Ssizes[0] = 30;
	     AttrType [] Rtypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
	     };
	     
	     short  []  Rsizes = new short[1] ;
	     Rsizes[0] = 15;
	     AttrType [] Btypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrString), 
	     };
	     
	     short  []  Bsizes = new short[2];
	     Bsizes[0] =30;
	     Bsizes[1] =20;
	     
	     
	     AttrType [] Jtypes = { //Pi(sname, bid)
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrInteger), 
	     };
	     
	     short  []  Jsizes = new short[1];
	     Jsizes[0] = 30;
	     AttrType [] JJtype = {
		new AttrType(AttrType.attrString), 
	     };
	     
	     short [] JJsize = new short[1];
	     JJsize[0] = 30; 
	     
	     
	     
	     FldSpec []  proj1 = { // S.sname, R.bid
		new FldSpec(new RelSpec(RelSpec.outer), 2),
		new FldSpec(new RelSpec(RelSpec.innerRel), 2)
	     }; 
	     
	     FldSpec [] proj2  = { //?
		new FldSpec(new RelSpec(RelSpec.innerRel), 2)
	     };
	     
	     FldSpec [] Sprojection = {
		new FldSpec(new RelSpec(RelSpec.outer), 1),
		new FldSpec(new RelSpec(RelSpec.outer), 2),
	       new FldSpec(new RelSpec(RelSpec.outer), 3),
	       new FldSpec(new RelSpec(RelSpec.outer), 4)
	     };

	     FileScan am = null;
	     try {
		am  = new FileScan("sailors.in", Stypes, Ssizes, 
				   (short)4, (short)4,
				   Sprojection, null);
	     }
	     catch (Exception e) {
		status = FAIL;
		System.err.println (""+e);
		e.printStackTrace();
	     }
	     
	     if (status != OK) {
		//bail out
		
		System.err.println ("*** Error setting up scan for sailors");
		Runtime.getRuntime().exit(1);
	     }
	     
	     NestedLoopsJoins inl = null;
	     try {
		inl = new NestedLoopsJoins (Stypes, 4, Ssizes,
					    Rtypes, 3, Rsizes,
					    10,
					  am, "reserves.in",
					    outFilter, null, proj1, 2);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	     }

	     NestedLoopsJoins nlj = null;
	     try {
		nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
					    Btypes, 3, Bsizes,
					    10,
					    inl, "boats.in",
					    outFilter2, null, proj2, 1);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	     }

	     TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
	     Sort sort_names = null;
	     try {
		sort_names = new Sort (JJtype,(short)1, JJsize,
				       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for sorting");
		System.err.println (""+e);
		Runtime.getRuntime().exit(1);
	     }
	     
	     
	     System.out.print( "After sorting the output tuples.\n");
	     
	     try {
		while ((t =sort_names.get_next()) !=null) {
		  t.print(JJtype);
		}
	     }catch (Exception e) {
		System.err.println ("*** Error preparing for get_next tuple");
		System.err.println (""+e);
		Runtime.getRuntime().exit(1);
	     }
	     
	     System.out.println ("\n"); 
	     try {
		sort_names.close();
	     }
	     catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	     }
	     
	     if (status != OK) {
		//bail out
		
		Runtime.getRuntime().exit(1);
	     }
	  }
   
   public void Query8() {
	   
	     System.out.print("**********************Query8 starting *********************\n");
	     boolean status = OK;
	     // Sailors, Boats, Reserves Queries.
	     System.out.print(""
				+ "  SELECT   B.bname\n"
				+ "  FROM     Sailors S, Boats B, Reserves R\n"
				+ "  WHERE    S.sid = R.sid AND R.bid = B.bid AND S.age >= 40 AND S.age <= 45 \n"
				+ "  ORDER BY B.bname\n\n"
				
				+ "Plan used:\n"
				+" Sort(Pi(bname) (B  |><|  Pi(sname, bid) (Sigma(40<=S.age<=45)  |><|  R)))\n\n");
	     
	     CondExpr [] outFilter  = new CondExpr[4];
	     outFilter[0] = new CondExpr();
	     outFilter[1] = new CondExpr();
	     outFilter[2] = new CondExpr();
	     outFilter[3] = new CondExpr();
	     CondExpr [] outFilter2 = new CondExpr[2];
	     outFilter2[0] = new CondExpr();
	     outFilter2[1] = new CondExpr();
	     
	     Query8_CondExpr(outFilter, outFilter2);
	     Tuple t = new Tuple();
	     t = null;
	     
	     AttrType [] Stypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrReal)
	     };

	     short []   Ssizes = new short[1];
	     Ssizes[0] = 30;
	     AttrType [] Rtypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
	     };
	     
	     short  []  Rsizes = new short[1] ;
	     Rsizes[0] = 15;
	     AttrType [] Btypes = {
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrString), 
	     };
	     
	     short  []  Bsizes = new short[2];
	     Bsizes[0] =30;
	     Bsizes[1] =20;
	     
	     
	     AttrType [] Jtypes = { //Pi(sname, bid)
		new AttrType(AttrType.attrString), 
		new AttrType(AttrType.attrInteger), 
	     };
	     
	     short  []  Jsizes = new short[1];
	     Jsizes[0] = 30;
	     AttrType [] JJtype = {
		new AttrType(AttrType.attrString), 
	     };
	     
	     short [] JJsize = new short[1];
	     JJsize[0] = 30; 
	     
	     
	     
	     FldSpec []  proj1 = {
		new FldSpec(new RelSpec(RelSpec.outer), 2),
		new FldSpec(new RelSpec(RelSpec.innerRel), 2)
	     }; 
	     
	     FldSpec [] proj2  = {
		new FldSpec(new RelSpec(RelSpec.innerRel), 2)
	     };
	     
	     FldSpec [] Sprojection = {
		new FldSpec(new RelSpec(RelSpec.outer), 1),
		new FldSpec(new RelSpec(RelSpec.outer), 2),
	       new FldSpec(new RelSpec(RelSpec.outer), 3),
	       new FldSpec(new RelSpec(RelSpec.outer), 4)
	     };
	     
	     FileScan am = null;
	     try {
		am  = new FileScan("sailors.in", Stypes, Ssizes, 
				   (short)4, (short)4,
				   Sprojection, null);
	     }
	     catch (Exception e) {
		status = FAIL;
		System.err.println (""+e);
		e.printStackTrace();
	     }
	     
	     if (status != OK) {
		//bail out
		
		System.err.println ("*** Error setting up scan for sailors");
		Runtime.getRuntime().exit(1);
	     }

	     NestedLoopsJoins inl = null;
	     try {
		inl = new NestedLoopsJoins (Stypes, 4, Ssizes,
					    Rtypes, 3, Rsizes,
					    10,
					  am, "reserves.in",
					    outFilter, null, proj1, 2);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	     }
	    
	     NestedLoopsJoins nlj = null;
	     try {
		nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
					    Btypes, 3, Bsizes,
					    10,
					    inl, "boats.in",
					    outFilter2, null, proj2, 1);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	     }
	     
	     TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
	     Sort sort_names = null;
	     try {
		sort_names = new Sort (JJtype,(short)1, JJsize,
				       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
	     }
	     catch (Exception e) {
		System.err.println ("*** Error preparing for sorting");
		System.err.println (""+e);
		Runtime.getRuntime().exit(1);
	     }
	     
	     
	     System.out.print( "After sorting the output tuples.\n");
	     
	     try {
		while ((t =sort_names.get_next()) !=null) {
		  t.print(JJtype);
		}
	     }catch (Exception e) {
		System.err.println ("*** Error preparing for get_next tuple");
		System.err.println (""+e);
		Runtime.getRuntime().exit(1);
	     }
	     
	     System.out.println ("\n"); 
	     try {
		sort_names.close();
	     }
	     catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	     }
	     
	     if (status != OK) {
		//bail out
		
		Runtime.getRuntime().exit(1);
	     }
	  }
   
   public void LoadDB(int nb_of_tuples) {
	    File2Heap(path_to_queries+"Q.txt","Q.in",nb_of_tuples);
	    File2Heap(path_to_queries+"R.txt","R.in",nb_of_tuples);
	    File2Heap(path_to_queries+"S.txt","S.in",nb_of_tuples);
   }
   
  public void Query1a(String filename) {

	  
	  System.out.print("**********************Query1a starting *********************\n");
	  boolean status = OK;
	  
	  printWriter.print("Query 1a: " + filename + "\n");
	  
	  CondExpr [] outFilter  = new CondExpr[2];
	  outFilter[0] = new CondExpr();
	  outFilter[1] = new CondExpr();
	  
	  File2SingleCondExpr(outFilter,filename);
      Tuple t = new Tuple();
      t = null;
	  
	    AttrType [] FirstTypes = new AttrType[4];
	    FirstTypes[0] = new AttrType (AttrType.attrInteger);
	    FirstTypes[1] = new AttrType (AttrType.attrInteger);
	    FirstTypes[2] = new AttrType (AttrType.attrInteger);
	    FirstTypes[3] = new AttrType (AttrType.attrInteger);
	    
	    short []   FirstSizes = new short[0];
	    AttrType [] SecondTypes = new AttrType[4];
	    SecondTypes[0] = new AttrType (AttrType.attrInteger);
	    SecondTypes[1] = new AttrType (AttrType.attrInteger);
	    SecondTypes[2] = new AttrType (AttrType.attrInteger);
	    SecondTypes[3] = new AttrType (AttrType.attrInteger);
	    
	    short []   SecondSizes = new short[1];
	    
	    FldSpec [] FirstProjection = new FldSpec[4];
	    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    FirstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    FirstProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    FirstProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
	    
	    FldSpec [] Jprojection = new FldSpec[2];
	    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
	    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));

	    AttrType [] Jtypes = new AttrType[2];
	    Jtypes[0] = new AttrType (AttrType.attrInteger);
	    Jtypes[1] = new AttrType (AttrType.attrInteger);

	    short [] Jsizes = new short[2];

	    FileScan am = null;	    

	    try {
	        am  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
	  				  (short)4, (short)4,
	  				  FirstProjection, null);
	    }
	    catch (Exception e) {
	        status = FAIL;
	        System.err.println (""+e);
	      }
	    

	    long startTime = System.currentTimeMillis();
	    NestedLoopsJoins nlj = null;
	      try {
		nlj = new NestedLoopsJoins (FirstTypes, 4, FirstSizes,
					    SecondTypes, 4, SecondSizes,
					    10,
					    am, GetTables(filename).get(1) + ".in",
					    outFilter, null, Jprojection, 2);
	      }
	      catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	      }

	    int count = 0;
	      try {
	  		while ((t =nlj.get_next()) !=null) {
	  		  //t.print(Jtypes);
	  			printWriter.print(t.toString(Jtypes) + "\n");
	  			count++;
	  		}
	  	     }catch (Exception e) {
	  		System.err.println ("*** Error preparing for get_next tuple");
	  		System.err.println (""+e);
	  		Runtime.getRuntime().exit(1);
	  	     }
	      
	        System.out.println(count);
		    long stopTime = System.currentTimeMillis();
		    System.out.println("Execution time (NLJ): " + (stopTime - startTime) + " ms");
	  	     
	  	     System.out.println ("\n"); 
	  	     try {
	  		nlj.close();
	  	     }
	  	     catch (Exception e) {
	  		status = FAIL;
	  		e.printStackTrace();
	  	     }
	  	     
	  	     if (status != OK) {
	  		//bail out
	  		
	  		Runtime.getRuntime().exit(1);
	  	     }
  }
  
  public void Query1b(String filename) {
	  System.out.print("**********************Query1b starting *********************\n");
	  boolean status = OK;
	  
	  printWriter.print("Query 1b: " + filename + "\n");
	  
	  CondExpr [] outFilter  = new CondExpr[3];
	  outFilter[0] = new CondExpr();
	  outFilter[1] = new CondExpr();
	  outFilter[2] = new CondExpr();
	  
	  File2DoubleCondExpr(outFilter,filename);
      Tuple t = new Tuple();
      t = null;
      
	    AttrType [] FirstTypes = new AttrType[4];
	    FirstTypes[0] = new AttrType (AttrType.attrInteger);
	    FirstTypes[1] = new AttrType (AttrType.attrInteger);
	    FirstTypes[2] = new AttrType (AttrType.attrInteger);
	    FirstTypes[3] = new AttrType (AttrType.attrInteger);
	    
	    short []   FirstSizes = new short[0];
	    AttrType [] SecondTypes = new AttrType[4];
	    SecondTypes[0] = new AttrType (AttrType.attrInteger);
	    SecondTypes[1] = new AttrType (AttrType.attrInteger);
	    SecondTypes[2] = new AttrType (AttrType.attrInteger);
	    SecondTypes[3] = new AttrType (AttrType.attrInteger);
	    
	    short []   SecondSizes = new short[1];
	    
	    FldSpec [] FirstProjection = new FldSpec[4];
	    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    FirstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    FirstProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    FirstProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
	    

	    FldSpec [] Jprojection = new FldSpec[2];
	    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
	    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));
	    
	    AttrType [] Jtypes = new AttrType[2];
	    Jtypes[0] = new AttrType (AttrType.attrInteger);
	    Jtypes[1] = new AttrType (AttrType.attrInteger);
	    
	    FileScan am = null;	    
	    
	    try {
	        am  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
	  				  (short)4, (short)4,
	  				  FirstProjection, null);
	    }
	    catch (Exception e) {
	        status = FAIL;
	        System.err.println (""+e);
	      }
	    
	    long startTime = System.currentTimeMillis();
	    
	    NestedLoopsJoins nlj = null;
	      try {
		nlj = new NestedLoopsJoins (FirstTypes, 4, FirstSizes,
					    SecondTypes, 4, SecondSizes,
					    10,
					    am, GetTables(filename).get(1) + ".in",
					    outFilter, null, Jprojection, 2);
	      }
	      catch (Exception e) {
		System.err.println ("*** Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	      }

	      
	      try {
	    	  int cnt=0;
	  		while ((t =nlj.get_next()) !=null) {
	  		  //t.print(Jtypes);
	  		  printWriter.print("Query 1b: " + filename + "\n");
	  		  cnt++;
	  		}
	  		System.out.println(cnt);
	  	     }catch (Exception e) {
	  		System.err.println ("*** Error preparing for get_next tuple");
	  		System.err.println (""+e);
	  		Runtime.getRuntime().exit(1);
	  	     }
	      
	      long stopTime = System.currentTimeMillis();
	      System.out.println("Execution time (DoublePredicate NLJ): " + (stopTime - startTime) + " ms");
	  	     
	  	     System.out.println ("\n"); 
	  	     try {
	  		nlj.close();
	  	     }
	  	     catch (Exception e) {
	  		status = FAIL;
	  		e.printStackTrace();
	  	     }
	  	     
	  	     if (status != OK) {
	  		//bail out
	  		
	  		Runtime.getRuntime().exit(1);
	  	     }
  }
  
public void Query2a(String filename) {
	  
	  System.out.println("**********************Query2a starting *********************\n");
	  boolean status = OK;
	  
	  printWriter.print("Query 2a: " + filename + "\n");

	  CondExpr [] outFilter  = new CondExpr[2];
	  outFilter[0] = new CondExpr();
	  outFilter[1] = new CondExpr();
	  
	  File2SingleCondExpr(outFilter,filename);
      Tuple t = new Tuple();
      t = null;
	  
	    AttrType [] FirstTypes = new AttrType[4];
	    FirstTypes[0] = new AttrType (AttrType.attrInteger);
	    FirstTypes[1] = new AttrType (AttrType.attrInteger);
	    FirstTypes[2] = new AttrType (AttrType.attrInteger);
	    FirstTypes[3] = new AttrType (AttrType.attrInteger);

	    short []   FirstSizes = new short[0];
	    
	    FldSpec [] FirstProjection = new FldSpec[4];
	    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    FirstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    FirstProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    FirstProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
	    
	    FldSpec [] Jprojection = new FldSpec[2];
	    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
	    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));

	    AttrType [] Jtypes = new AttrType[2];
	    Jtypes[0] = new AttrType (AttrType.attrInteger);
	    Jtypes[1] = new AttrType (AttrType.attrInteger);

	    short [] Jsizes = new short[0];

	    FileScan am = null;

	    try {
	        am  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
	  				  (short)4, (short)4,
	  				  FirstProjection, null);
	    }
	    
	    catch (Exception e) {
	        status = FAIL;
	        System.err.println (""+e);
	    }
	    
	      IESelfJoinSinglePredicate isj = null;
	      long startTime = System.currentTimeMillis();
	      try {
		isj = new IESelfJoinSinglePredicate (FirstTypes, 4, FirstSizes,
					    10,
					    am, outFilter, Jprojection, 2, false);
	      }
	      catch (Exception e) {
		System.err.println ("* Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	      }
	      long stopTime = System.currentTimeMillis();
	      System.out.println("Execution time (Query n°2a IESelfJoinSinglePredicate): " + (stopTime - startTime) + " ms");

		      System.out.println("OUTPUT OF ISJ");
		      
		      System.out.println(isj.getSize());
		      
		      try {
		  		while ((t =isj.get_next()) !=null) {
		  		  //t.print(Jtypes);
		  			printWriter.print(t.toString(Jtypes) + "\n");
		  		}
		  	     }catch (Exception e) {
		  		System.err.println ("*** Error preparing for get_next tuple");
		  		System.err.println (""+e);
		  		Runtime.getRuntime().exit(1);
		  	    }
		      
		      try {
			  		isj.close();
			  	     }
			  	     catch (Exception e) {
			  		status = FAIL;
			  		e.printStackTrace();
			  }

	  	     
	  	     if (status != OK) {
	  		//bail out
	  		
	  		Runtime.getRuntime().exit(1);
	  	     }
  }
  
public void Query2aOptim(String filename) {
	  
	  System.out.println("**********************Query2aOptim starting *********************\n");
	  boolean status = OK;
	  
	  printWriter.print("Query 2aOptim: " + filename + "\n");
	  
	  CondExpr [] outFilter  = new CondExpr[2];
	  outFilter[0] = new CondExpr();
	  outFilter[1] = new CondExpr();
	  
	  File2SingleCondExpr(outFilter,filename);
      Tuple t = new Tuple();
      t = null;
	  
	    AttrType [] FirstTypes = new AttrType[4];
	    FirstTypes[0] = new AttrType (AttrType.attrInteger);
	    FirstTypes[1] = new AttrType (AttrType.attrInteger);
	    FirstTypes[2] = new AttrType (AttrType.attrInteger);
	    FirstTypes[3] = new AttrType (AttrType.attrInteger);

	    short []   FirstSizes = new short[0];
	    
	    AttrType [] SecondTypes = new AttrType[1];
	    SecondTypes[0] = new AttrType (AttrType.attrInteger);

	    short []   SecondSizes = new short[0];
	    
	    FldSpec [] FirstProjection = new FldSpec[1];
	    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), GetColumns(filename).get(2));
	    
	    FldSpec [] Jprojection = new FldSpec[2];
	    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
	    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));

	    AttrType [] Jtypes = new AttrType[2];
	    Jtypes[0] = new AttrType (AttrType.attrInteger);
	    Jtypes[1] = new AttrType (AttrType.attrInteger);

	    short [] Jsizes = new short[0];

	    FileScan am = null;

	    try {
	        am  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
	  				  (short)4, (short)1,
	  				  FirstProjection, null);
	    }
	    
	    catch (Exception e) {
	        status = FAIL;
	        System.err.println (""+e);
	      }

	      IESelfJoinSinglePredicate isj = null;
	      long startTime = System.currentTimeMillis();
	      try {
		isj = new IESelfJoinSinglePredicate (SecondTypes, 1, SecondSizes,
						10,
					    am, outFilter, Jprojection, 2, true);
	      }
	      catch (Exception e) {
		System.err.println ("* Error preparing for nested_loop_join");
		System.err.println (""+e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	      }
	      long stopTime = System.currentTimeMillis();
	      System.out.println("Execution time (Query n°2aOptim IESelfJoinSinglePredicate): " + (stopTime - startTime) + " ms");

	  	     
		      System.out.println("OUTPUT OF ISJ");
		      
		      System.out.println(isj.getSize());
		      
		      try {
		  		while ((t =isj.get_next()) !=null) {
		  		  //t.print(Jtypes);
		  			printWriter.print(t.toString(Jtypes) + "\n");
		  		}
		  	     }catch (Exception e) {
		  		System.err.println ("*** Error preparing for get_next tuple");
		  		System.err.println (""+e);
		  		Runtime.getRuntime().exit(1);
		  	    }
		      
		      try {
			  		isj.close();
			  	     }
			  	     catch (Exception e) {
			  		status = FAIL;
			  		e.printStackTrace();
			  }

	  	     
	  	     if (status != OK) {
	  		//bail out
	  		
	  		Runtime.getRuntime().exit(1);
	  	     }
  }
  
	public void Query2b(String filename) {
		
		  System.out.print("**********************Query2b starting *********************\n");
		  boolean status = OK;
		  
		  printWriter.print("Query 2b: " + filename + "\n");
		  
		  CondExpr [] outFilter  = new CondExpr[3];
		  outFilter[0] = new CondExpr();
		  outFilter[1] = new CondExpr();
		  outFilter[2] = new CondExpr();
		  
		  File2DoubleCondExpr(outFilter,filename);
	      Tuple t = new Tuple();
	      t = null;
	      
		    AttrType [] FirstTypes = new AttrType[4];
		    FirstTypes[0] = new AttrType (AttrType.attrInteger);
		    FirstTypes[1] = new AttrType (AttrType.attrInteger);
		    FirstTypes[2] = new AttrType (AttrType.attrInteger);
		    FirstTypes[3] = new AttrType (AttrType.attrInteger);
		    
		    short []   FirstSizes = new short[0];
		    
		    AttrType [] SecondTypes = new AttrType[4];
		    SecondTypes[0] = new AttrType (AttrType.attrInteger);
		    SecondTypes[1] = new AttrType (AttrType.attrInteger);
		    SecondTypes[2] = new AttrType (AttrType.attrInteger);
		    SecondTypes[3] = new AttrType (AttrType.attrInteger);
		    
		    short []   SecondSizes = new short[0];
		    
		    FldSpec [] FirstProjection = new FldSpec[4];
		    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		    FirstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		    FirstProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		    FirstProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		    
		    FldSpec [] SecondProjection = new FldSpec[4];
		    SecondProjection[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		    SecondProjection[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		    SecondProjection[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
		    SecondProjection[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
		    
		    FldSpec [] Jprojection = new FldSpec[2];
		    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
		    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));
		    
		    AttrType [] Jtypes = new AttrType[2];
		    Jtypes[0] = new AttrType (AttrType.attrInteger);
		    Jtypes[1] = new AttrType (AttrType.attrInteger);
		    
			FileScan am = null;
			FileScan am2 = null;

		    try {
		        am  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		        
		        am2  = new FileScan(GetTables(filename).get(0) + ".in", SecondTypes, SecondSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		    } catch (Exception e) {
		        status = FAIL;
		        System.err.println (""+e);
		      }
		    
		    long startTime = System.currentTimeMillis();
		      
		      IESelfJoinDoublePredicate iesl = null;
		     try {
			iesl = new IESelfJoinDoublePredicate (FirstTypes, 4, FirstSizes,
						    SecondTypes, 4, SecondSizes,
						    10,
						    am, am2,
						    outFilter, Jprojection, 2, false);
		      }
		      catch (Exception e) {
			System.err.println ("*** Error preparing for IESelfJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		  	}
		     
		      long stopTime = System.currentTimeMillis();
		      System.out.println("Execution time (Query n°2b IESelfJoinDoublePredicate): " + (stopTime - startTime) + " ms");
		  	     
		  	   System.out.println("IESelfJoinDoublePredicate Result");
			   System.out.println(iesl.getSize());
			   
			   try {
			  		while ((t =iesl.get_next()) !=null) {
			  		  //t.print(Jtypes);
			  			printWriter.print(t.toString(Jtypes) + "\n");
			  		}
			  	     }catch (Exception e) {
			  		System.err.println ("*** Error preparing for get_next tuple");
			  		System.err.println (""+e);
			  		Runtime.getRuntime().exit(1);
			  	    }
			   
			  	     System.out.println ("\n"); 
			  	     try {
			  	    	iesl.close();
			  	     }
			  	     catch (Exception e1) {
			  		 status = FAIL;
			  		 e1.printStackTrace();
			  	     }
			  	     
			  	     if (status != OK) {
			  		//bail out
			  		
			  		Runtime.getRuntime().exit(1);
			  	     }
		  	     }
	
	public void Query2bOptim(String filename) {
		
		  System.out.print("**********************Query2bOptim starting *********************\n");
		  boolean status = OK;
		  
		  printWriter.print("Query 2bOptim: " + filename + "\n");
		  
		  CondExpr [] outFilter  = new CondExpr[3];
		  outFilter[0] = new CondExpr();
		  outFilter[1] = new CondExpr();
		  outFilter[2] = new CondExpr();
		  
		  File2DoubleCondExpr(outFilter,filename);
	      Tuple t = new Tuple();
	      t = null;
	      
		    AttrType [] FirstTypes = new AttrType[4];
		    FirstTypes[0] = new AttrType (AttrType.attrInteger);
		    FirstTypes[1] = new AttrType (AttrType.attrInteger);
		    FirstTypes[2] = new AttrType (AttrType.attrInteger);
		    FirstTypes[3] = new AttrType (AttrType.attrInteger);
		    
		    short []   FirstSizes = new short[0];
		    
		    FldSpec [] FirstProjection1 = new FldSpec[2];
		    FirstProjection1[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(2));
		    FirstProjection1[1] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(4));

		    
		    FldSpec [] FirstProjection2 = new FldSpec[2];
		    FirstProjection2[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(2));
		    FirstProjection2[1] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(4));
		    
		    AttrType [] SecondTypes = new AttrType[2];
		    SecondTypes[0] = new AttrType (AttrType.attrInteger);
		    SecondTypes[1] = new AttrType (AttrType.attrInteger);
		    
		    short []   SecondSizes = new short[0];
		    
		    FldSpec [] Jprojection = new FldSpec[2];
		    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
		    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));
		    
		    AttrType [] Jtypes = new AttrType[2];
		    Jtypes[0] = new AttrType (AttrType.attrInteger);
		    Jtypes[1] = new AttrType (AttrType.attrInteger);
		    
			FileScan am1 = null;
			FileScan am2 = null;

		    try {
		        am1  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
		  				  (short)4, (short)2,
		  				  FirstProjection1, null);
		        
		        am2  = new FileScan(GetTables(filename).get(0) + ".in", FirstTypes, FirstSizes, 
		  				  (short)4, (short)2,
		  				  FirstProjection2, null);
		    } catch (Exception e) {
		        status = FAIL;
		        System.err.println (""+e);
		      }
		    
		    long startTime = System.currentTimeMillis();
		      
		      IESelfJoinDoublePredicate iesl = null;
		     try {
			iesl = new IESelfJoinDoublePredicate (SecondTypes, 2, SecondSizes,
						    SecondTypes, 2, SecondSizes,
						    40,
						    am1, am2,
						    outFilter, Jprojection, 2, true);
		      }
		      catch (Exception e) {
			System.err.println ("* Error preparing for IESelfJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		      
		  	}
		     
		      long stopTime = System.currentTimeMillis();
		      System.out.println("Execution time (Query n°2b IESelfJoinDoublePredicate): " + (stopTime - startTime) + " ms");
		  	     
		  	   System.out.println("IESelfJoinDoublePredicate Result");
			   System.out.println(iesl.getSize());
			   
			   try {
			  		while ((t =iesl.get_next()) !=null) {
			  		  //t.print(Jtypes);
			  			printWriter.print(t.toString(Jtypes) + "\n");
			  		}
			  	     }catch (Exception e) {
			  		System.err.println ("*** Error preparing for get_next tuple");
			  		System.err.println (""+e);
			  		Runtime.getRuntime().exit(1);
			  	     }
			  	     
			  	     System.out.println ("\n"); 
			  	     try {
			  	    	iesl.close();
			  	     }
			  	     catch (Exception e1) {
			  		 status = FAIL;
			  		 e1.printStackTrace();
			  	     }
			  	     
			  	     if (status != OK) {
			  		//bail out
			  		
			  		Runtime.getRuntime().exit(1);
			  	     }
		  	     }

	public void Query2c(String filename) {
		  System.out.print("**********************Query2c starting *********************\n");
		  boolean status = OK;
		  
		  printWriter.print("Query 2c: " + filename + "\n");
		  
		  CondExpr [] outFilter  = new CondExpr[3];
		  outFilter[0] = new CondExpr();
		  outFilter[1] = new CondExpr();
		  outFilter[2] = new CondExpr();
		  
		  File2DoubleCondExpr(outFilter,filename);

	      ArrayList<Character> tables = GetTables(filename);
	      
		  Tuple t = new Tuple();
	      t = null;
	      
	      AttrType [] FirstTypes = new AttrType[4];
		    FirstTypes[0] = new AttrType (AttrType.attrInteger);
		    FirstTypes[1] = new AttrType (AttrType.attrInteger);
		    FirstTypes[2] = new AttrType (AttrType.attrInteger);
		    FirstTypes[3] = new AttrType (AttrType.attrInteger);
		    
		    short []   FirstSizes = new short[0];
		    
		    AttrType [] SecondTypes = new AttrType[4];
		    SecondTypes[0] = new AttrType (AttrType.attrInteger);
		    SecondTypes[1] = new AttrType (AttrType.attrInteger);
		    SecondTypes[2] = new AttrType (AttrType.attrInteger);
		    SecondTypes[3] = new AttrType (AttrType.attrInteger);
		    
		    short []   SecondSizes = new short[0];
		    
		    FldSpec [] FirstProjection = new FldSpec[4];
		    FirstProjection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		    FirstProjection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		    FirstProjection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		    FirstProjection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		    
		    FldSpec [] Jprojection = new FldSpec[2];
		    Jprojection[0] = new FldSpec(new RelSpec(RelSpec.outer),GetColumns(filename).get(0));
		    Jprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),GetColumns(filename).get(1));
		    
		    AttrType [] Jtypes = new AttrType[2];
		    Jtypes[0] = new AttrType (AttrType.attrInteger);
		    Jtypes[1] = new AttrType (AttrType.attrInteger);
		    
			FileScan am1 = null;
			FileScan am2 = null;
			FileScan am3 = null;
			FileScan am4 = null;

		    try {
		        am1  = new FileScan(tables.get(0) + ".in", FirstTypes, FirstSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		        am2  = new FileScan(tables.get(1) + ".in", SecondTypes, SecondSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		        am3  = new FileScan(tables.get(0) + ".in", FirstTypes, FirstSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		        am4  = new FileScan(tables.get(1) + ".in", SecondTypes, SecondSizes, 
		  				  (short)4, (short)4,
		  				  FirstProjection, null);
		    } catch (Exception e) {
		        status = FAIL;
		        System.err.println (""+e);
		      }
		    
		    long startTime = System.currentTimeMillis();
		    
		      IEJoinDoublePredicate iel = null;
		     try {
					iel = new IEJoinDoublePredicate (FirstTypes, 4, FirstSizes,
						    SecondTypes, 4, SecondSizes,
						    10,
						    am1, am2, am3, am4,
						    outFilter, Jprojection, 2);
		      }
		      catch (Exception e) {
			System.err.println ("* Error preparing for IEJoinDoublePredicate");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		      
		  	}
		     long stopTime = System.currentTimeMillis();
		     System.out.println("Execution time (Query n°2c IEJoinDoublePredicate): " + (stopTime - startTime) + " ms");
		     
		     iel.getSize();
		     
			   try {
			  		while ((t =iel.get_next()) !=null) {
			  		  //t.print(Jtypes);
			  			printWriter.print(t.toString(Jtypes) + "\n");
			  		}
			  	     }catch (Exception e) {
			  		System.err.println ("*** Error preparing for get_next tuple");
			  		System.err.println (""+e);
			  		Runtime.getRuntime().exit(1);
			  	     }
		     
			  	     try {
			  	    	iel.close();
			  	     }
			  	     catch (Exception e1) {
			  		 status = FAIL;
			  		 e1.printStackTrace();
			  	     }
			  	     
			  	     System.out.println(iel.getSize());
			  	     
			  	     if (status != OK) {
			  		//bail out
			  		
			  		Runtime.getRuntime().exit(1);
			  	     }
}
		     
  
  
  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }
}

public class JoinTest
{
  public static void main(String argv[]) 
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver jjoin = new JoinsDriver();

    sortstatus = jjoin.runTests(400);
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}
