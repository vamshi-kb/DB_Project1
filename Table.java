
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key. 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.TREE_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        return switch (mType) {
        case TREE_MAP    -> new TreeMap <> ();
        case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
//      case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
        default          -> null;
        }; // switch
    } // makeMap

    /*
    ***********************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1  the first array
     * @param arr2  the second array
     * @return  a wider array containing all the values from arr1 and arr2
     */
    public static <T> T [] concat (T [] arr1, T [] arr2)
    {
        T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
        arraycopy (arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();

    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /*
    ***********************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /*
    ***********************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        var attrs     = attributes.split (" ");
        var colDomain = extractDom (match (attrs), domain);
        var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
        for(int i = 0; i < tuples.size(); i++){
            Comparable [] array = new Comparable[attrs.length];
            for(int j = 0; j < attrs.length; j++){
                int attrPos = col(attrs[j]);
                array[j] = tuples.get(i)[attrPos];
            }
            rows.add(array);
        }
        for(int x = 0; x < rows.size(); x++){
            for(int y = x+1; y < rows.size()-1; y++){
                if(rows.get(x).equals(rows.get(y))){
                    rows.remove(y);
                    x -= 1;
                }
            }
        }

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        for(int i = 0; i < tuples.size(); i++){
            if(tuples.get(i).equals(index.get(keyVal))){
                rows.add(tuples.get(i));
            }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
        boolean copyRow=true;

        //Adding all rows from table 1.
        for (Comparable[] tup1 : tuples) {
            rows.add(tup1);
        }

        for( int i=0; i<table2.tuples.size() ; i++) {
            //check if tuple is already in table 1
            for (int j = 0; j < tuples.size(); j++) {
                if (tuples.get(j)==table2.tuples.get(i)) {copyRow=false;}
            }
            //add row to table 1 if there is no duplicate
            if(copyRow){ rows.add(table2.tuples.get(i)); }
            else{ copyRow=true; } //reset
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        boolean copyRow=true;

        for( int i=0; i<tuples.size() ; i++) {
            //check if tuple is in table 2
            for (int j = 0; j < table2.tuples.size(); j++) {
                if (tuples.get(i)==table2.tuples.get(j)) {copyRow=false;}
            }
            //add row to new table row if tuple exists in both tables
            if(copyRow){ rows.add(tuples.get(i)); }
            else{ copyRow=true; }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.  Implement using
     * a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");
        var rows    = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D
        int [] t_attrsPos = new int [t_attrs.length];
        int [] u_attrsPos = new int [u_attrs.length];

        //Grabbing the collumn postions of the keys from each table.
        for(int i = 0; i < t_attrs.length; i++) { t_attrsPos[i] = col(t_attrs[i]); }

        for(int i = 0; i < u_attrs.length; i++) { u_attrsPos[i] = table2.col(u_attrs[i]); }



        //check domains
        for(int i = 0; i < t_attrs.length; i++) {
            String d1 = domain[t_attrsPos[i]].getName();
            String d2 = table2.domain[u_attrsPos[i]].getName();
            if ( ! d1.equals(d2)) {
                out.println("The domain of attribute " + attribute[t_attrsPos[i]] + " is " + d1);
                out.println("The domain of attribute " + table2.attribute[u_attrsPos[i]] + " is " + d2);
                out.println("These domain dont match!");
                return null;
            }
        }

        //Creating the new tuples
        for (Comparable[] tup1 : tuples) {
            for(Comparable[] tup2 : table2.tuples) {

                //int to be used to assure that each column is matched
                int match = 0;

                for(int k = 0; k < t_attrs.length; k++) {
                    if( tup1[t_attrsPos[k]] == tup2[u_attrsPos[k]]) {match++;}
                }

                if(match == t_attrs.length) {
                    //Creating new tuple
                    Comparable[] row = ArrayUtil.concat(tup1,tup2);
                    rows.add(row);
                }

            }
        }

        String[] newAttr = table2.attribute;
        for(int i = 0; i < attribute.length; i++) {
            for(int j = 0; j < table2.attribute.length; j++) {
                if(attribute[i].equals(table2.attribute[j])){
                    newAttr[j] = table2.attribute[j] + "2";
                }
            }

        }

        return new Table (name + count++, concat (attribute, table2.attribute),
                                          concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        return null;
    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {

        //  D O   N O T   I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D 
        int numDupCol = 0;
        for(int i = 0; i< table2.attribute.length; i++) {
            if( ! (col(table2.attribute[i]) == -1)) {
                numDupCol++;
            }
        }

        int [] t_keysPos = new int [numDupCol];
        int [] u_keysPos = new int [numDupCol];

        //Grabbing the collumn postions of the keys from each table.
        for (int i = 0; i > table2.attribute.length; i++) { t_keysPos[i] = col(table2.attribute[i]); }
        for(int i = 0; i < attribute.length; i++) { u_keysPos[i] = table2.col(attribute[i]); }

        //Checking for special case
        boolean intersection = false;
        if(table2.attribute.length==attribute.length){
            if(numDupCol == attribute.length){
                intersection = true;
            }
        }

        String[] newAttr = new String[table2.attribute.length - numDupCol];
        Class[] newDomain = new Class[table2.attribute.length - numDupCol];
        for(int i = 0; i<newAttr.length; i++){
            for(int j = 0; j <attribute.length; j++) {
                for(int k = 0; k < table2.attribute.length; k++){
                    if(!(attribute[j].equals(table2.attribute[k]))){
                        newAttr[i] = table2.attribute[k];
                        newDomain[i] = table2.domain[k];
                    }
                }
            }
        }

        if(numDupCol == 0) {
            //CrossProduct
            for (Comparable[] tup1 : tuples) {
                for (Comparable[] tup2 : table2.tuples){
                    Comparable[] newTup2 = extract(tup2, newAttr);
                    rows.add(ArrayUtil.concat(tup1,newTup2));
                }

                return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                        ArrayUtil.concat (domain, table2.domain), key, rows);
            }
        } else if(! intersection) {
            //Checking domains

            for(int i = 0; i < t_keysPos.length; i++) {
                String d1 = domain[t_keysPos[i]].getName();
                String d2 = table2.domain[u_keysPos[i]].getName();
                if ( ! d1.equals(d2)) {
                    out.println("The domain of attribute " + attribute[t_keysPos[i]] + " is " + d1);
                    out.println("The domain of attribute " + table2.attribute[u_keysPos[i]] + " is " + d2);
                    out.println("These domain dont match!");
                    return null;
                }
            }
            for (Comparable[] tup1 : tuples) {
                for(Comparable[] tup2 : table2.tuples) {

                    //int to be used to assure that each column is matched
                    int match = 0;

                    for(int k = 0; k < t_keysPos.length; k++) {
                        if( tup1[t_keysPos[k]] == tup2[u_keysPos[k]]) {match++;}
                    }

                    if(match == t_keysPos.length) {

                        rows.add(ArrayUtil.concat(tup1, extract(tup2, newAttr)));
                    }

                }
            }

            return new Table (name + count++, ArrayUtil.concat (attribute, newAttr),
                    ArrayUtil.concat (domain, newDomain), key, rows);

        }
        else if(intersection) {

            //Intersection
            for (Comparable[] tup1 : tuples) {

                for(Comparable[] tup2: table2.tuples){
                    Comparable[] testTup = extract(tup2, attribute);

                    boolean addRow = true;


                    for(int i = 0; i < attribute.length; i++) {
                        if(!(testTup[i] == tup1[i])) {
                            addRow = false;
                        }
                    }
                    if(addRow){rows.add(testTup);}

                }
            }

            return new Table (name + count++, attribute, domain, key, rows);

        }

        // FIX - eliminate duplicate columns
        return new Table (name + count++, concat (attribute, table2.attribute),
                                          concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (var i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        out.print ("| ");
        for (var a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        for (var tup : tuples) {
            out.print ("| ");
            for (var attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /*
    ***********************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

} // Table class

