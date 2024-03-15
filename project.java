package dwhproject;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;


public class project {
    private static Map<Integer, List<Transaction>> multiHashTable = new HashMap<>();
    //private static Queue<Integer> transactionQueue = new LinkedList<>();
    private static final int BUFFER_SIZE = 1000;
  //  private static final int BUFFER_SIZE1 = 10;

    private static final ArrayBlockingQueue<Transaction> buffer = new ArrayBlockingQueue<>(BUFFER_SIZE);
    private static final ArrayBlockingQueue<Integer> transactionQueue = new ArrayBlockingQueue<>(BUFFER_SIZE);

    public static void main(String[] args) throws InterruptedException {
            try (Scanner scanner = new Scanner(System.in)) {
                System.out.println("Enter Username");
                String user = scanner.nextLine(); // user is "root"

                System.out.println("Enter Password");
                String pwd = scanner.nextLine(); // password is "root123"

                try (Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/transactions", user, pwd);
                	     Connection con1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/master_data", user, pwd);
                	     Connection con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/ELECTRONICA-DW", user, pwd)) {

                	    Thread generateThread = new Thread(() -> processAndGenerateTransactions(con));
                	    Thread processThread = new Thread(() -> {
                	        try {
                	            processMasterDataStream1(con1, con2);
                	        } catch (SQLException e) {
                	            e.printStackTrace();
                	        } catch (InterruptedException e) {
                	            e.printStackTrace();
                	        }
                	    });

                	    generateThread.start();
                	    processThread.start();

                	    generateThread.join();
                	    processThread.join();
                	} catch (SQLException e) {
                	    e.printStackTrace();
                	}
            }
        }
        
private static void fillQueueNHashMap() {
    	 System.out.println(" -----------------------------------------------");
        for (int i = 0; i < 1000; i++) {
            try {
            	// System.out.println(" -----------------------------------------------");
            	// Get transaction from the buffer
                Transaction transaction = buffer.take(); 
               // System.out.println(" -----------------------------------------------");
                int productID = transaction.getProductid();
                // Adding product id in the queue
                if (!transactionQueue.contains(productID)) {
                    transactionQueue.add(productID);
                }
                //Adding product id as key and further data as values in hash map
                List<Transaction> transactions = new ArrayList<>();
                transactions.add(transaction);
                multiHashTable.put(productID, transactions);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    
private static void processMasterDataStream1(Connection con1, Connection con2) throws SQLException, InterruptedException {
    System.out.println(" -----------------------------------------------");
    fillQueueNHashMap(); 
    System.out.println(" -----------------------------------------------");
    int countRemoved = 0;
    while (!transactionQueue.isEmpty()) {
        Integer productID = transactionQueue.poll(); 

        if (productID != null) {
            int intValue = productID.intValue();
        }

        String sql = "SELECT * FROM master_data.master_data WHERE productID >= " + productID + " LIMIT 10;";
        try (Statement statement = con1.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            List<Transaction> transactions = multiHashTable.get(productID);

            while (resultSet.next()) {
                int retrievedProductID = resultSet.getInt("productID");
                String productName = resultSet.getString("productName");
                String productPriceString = resultSet.getString("productPrice").replace("$", "");
                double productPrice = Double.parseDouble(productPriceString);
                int supplierID = resultSet.getInt("supplierID");
                String supplierName = resultSet.getString("supplierName");
                int storeID = resultSet.getInt("storeID");
                String storeName = resultSet.getString("storeName");
               


                if (retrievedProductID == productID) {
                    if (transactions != null) {
                        for (Transaction tr : transactions) {
                        	// String insertSalesFact =	"INSERT INTO Timee(OrderID, day, month, year,datee) VALUES (?, ?, ?, ?,?);";
//                           String insertSalesFact = "Insert into SalesFact (OrderID, ProductID, timeID, CustomerID, StoreID, SupplierID, QuantityOrdered,datee, ProductPrice)VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)";
//                            PreparedStatement pstmt = con2.prepareStatement(insertSalesFact);
//
//                            pstmt.setInt(1, tr.Orderid);  
//                            pstmt.setInt(2, tr.productid); 
//                            pstmt.setInt(3, tr.day);
//                            pstmt.setInt(4, tr.custid); 
//                            pstmt.setInt(5, storeID); 
//                             pstmt.setInt(6, supplierID); 
//                              pstmt.setInt(7, tr.quantity_ordered);
//                              
//                            LocalDateTime localDateTime = tr.date;
//                            Timestamp time = Timestamp.valueOf(localDateTime);
//                            pstmt.setTimestamp(8, time);
//                            pstmt.setDouble(9, productPrice); 
                        	 System.out.println("Order id" + tr.Orderid);
                        	 System.out.println("Product id" + productID);
                        	
//                           
//                          
//
//                            pstmt.executeUpdate();
//                            System.out.println("Inserted");

                           // pstmt.close();
                        }
                    } else {
                        System.out.println("Transactions not matched" + productID);
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        transactionQueue.remove(productID); 
        multiHashTable.remove(productID);
        countRemoved++;

        if (!buffer.isEmpty()&& countRemoved <= BUFFER_SIZE) {
            try {
                Transaction t = buffer.take();
                transactionQueue.add(t.productid);
                addValueToKeyInMultiHashTable(t.productid, t);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

    private static void addValueToKeyInMultiHashTable(int key, Transaction value) {
       
        if (multiHashTable.containsKey(key)) {
            
            multiHashTable.get(key).add(value);
        } else {
            
            List<Transaction> newList = new ArrayList<>();
            newList.add(value);
            multiHashTable.put(key, newList);
        }
    }
    public static void processAndGenerateTransactions(Connection con) {
        try {
            Statement statement = con.createStatement();
            String sql = "SELECT * FROM transactions.transactions";
            ResultSet resultSet = statement.executeQuery(sql);

            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .appendPattern("MM/dd/")
                    .optionalStart()
                    .appendPattern("[uuuu ][yyyy ][yy ]HH:mm")
                    .optionalEnd()
                    .toFormatter();

            //List<Transaction> buffer = new ArrayList<>();

            while (resultSet.next()) {
                int orderId = resultSet.getInt("Order ID");
                String orderDateStr = resultSet.getString("Order Date");
                LocalDateTime orderDate = LocalDateTime.parse(orderDateStr, formatter);
                int productId = resultSet.getInt("ProductID");
                int customerId = resultSet.getInt("CustomerID");
                String customerName = resultSet.getString("CustomerName");
                String gender = resultSet.getString("Gender");
                int quantityOrdered = resultSet.getInt("Quantity Ordered");
                
                int day = orderDate.getDayOfMonth();
                int month = orderDate.getMonthValue();
                int year = orderDate.getYear();
                Transaction transaction = new Transaction(orderId, orderDate, productId, customerId, 
                		customerName, gender, quantityOrdered, day, year, month);
                buffer.add(transaction);

               
            }
            resultSet.close();
            statement.close();
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
   
    static class MasterData {
        public String productName;
        public double productPrice;
        public int supplierID;
        public String supplierName;
        public int storeID;
        public String storeName;
        
        public MasterData() 
        {
            this.productName = "";
            this.productPrice = 0.0;
            this.supplierID = 0;
            this.supplierName = "";
            this.storeID = 0;
            this.storeName = "";
        }

        public MasterData(String productName, double productPrice, int supplierID, String supplierName, int storeID, String storeName) {
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierID = supplierID;
            this.supplierName = supplierName;
            this.storeID = storeID;
            this.storeName = storeName;
        }

    
        public void displayMasterData() {
            System.out.println("Product Name: " + productName);
            System.out.println("Product Price: " + productPrice);
            System.out.println("Supplier ID: " + supplierID);
            System.out.println("Supplier Name: " + supplierName);
            System.out.println("Store ID: " + storeID);
            System.out.println("Store Name: " + storeName);
        }

    }

  
    static class Transaction {
        public int Orderid;
        public LocalDateTime date;
        public int day;
        public int month;
        public int year;
        public int productid;
        public int custid;
        public String custname;
        public String gender;
        public int quantity_ordered;

      
        
        public Transaction() {
            this.Orderid = 0;
            this.date = LocalDateTime.now();
            this.productid = 0;
            this.custid = 0;
            this.custname = "";
            this.gender = "";
            this.quantity_ordered = 0;
            this.day = 0;
            this.month = 0;
            this.year = 0;
       
        }

        public Transaction(int oi, LocalDateTime date, int pi, int ci, String name, String gender, int qo, int day, int year, int month) {
            this.Orderid = oi;
            this.date = date;
            this.productid = pi;
            this.custid = ci;
            this.custname = name;
            this.gender = gender;
            this.quantity_ordered = qo;
            this.day = day;
            this.year = year;
            this.month = month;
          
         
        }
        
        public int getProductid() {
            return productid;
        }

    
       
        @Override
      
        	public String toString() 
        {
                return "Transaction{" +
                        "orderid=" + Orderid +
                        ", date='" + date + '\'' +
                        ", product id=" + productid +
                         
                        '}';
            
        
    }
    }
}
