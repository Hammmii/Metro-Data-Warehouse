

import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;



class DW_Project {

	
    private static final int chunkk = 100;

    public static void main(String[] args) {

    	
    	
    
    	
        Scanner scanner = new Scanner(System.in);

        
       
        System.out.print(" **** Welcome to DATAWAREHOUSE Project ****");
        
        String dbUrl =   "jdbc:mysql://localhost:3306/datawarehouse";


        System.out.print("\n Enter the database username: ");
        
        String dbUser = scanner.nextLine();

        System.out.print("Enter the database password: ");
        
        String dbPassword = scanner.nextLine();

        
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
        
        	System.out.println("Connected to the database.");

        	
        	
       
            Load_to_dw(connection, "transactions.csv", "transactions", 
                              "order_id, order_date, product_id, quantity_ordered, customer_id, time_id");
            
            
            
            
            Load_to_dw(connection, "products_data.csv", "products", 
                              "product_id, product_name, product_price, supplier_id, supplier_name, store_id, store_name");
            
            
            
            
            Load_to_dw(connection, "customers_data.csv", "customers", 
                              "customer_id, customer_name, gender");

            
            
            
            
            
            Populate_Product_Dimension(connection, "products_data.csv");
            
            Populate_Customers_Dimension(connection, "customers_data.csv");
            
            Populate_Time_Dimension(connection, "transactions.csv");

            
            
            
            System.out.println("Dimension tables populated successfully.");


            while (true) {
            	
                
            	List<Transaction> transactions = extractt_transc(connection);
                
            	if (transactions.isEmpty()) {
                
            		break; 
                }

                List<Product> products = loadProducts(connection);
                
                List<Customer> customers = customer_loading(connection);
                
                List<Time> times = loading_timee(connection);

                List<SalesFact> salesFacts = joinn_enrich(transactions, customers, products, times);
                
                inserrt_Fact(connection, salesFacts);
                
                
                
            }

            System.out.println("MESHJOIN process completed and sales_fact populated.");
            

           
        } catch (SQLException e) {
        	
            
        	System.err.println("Failed to connect to the database: " + e.getMessage());
        } finally {
        	
        	
            
        	scanner.close();
        	
        	
        }
        
        
    }


    private static void Load_to_dw(Connection connection, String fileName, String tableName, String columns) {
    	
    	
        String Pathh = "/Users/apple.store.pk/Desktop/University/Warehouse/" + fileName; 
        
        String Insertionn = String.format("INSERT INTO %s (%s) VALUES ", tableName, columns);

        try (BufferedReader br = new BufferedReader(new FileReader(Pathh));
        
        		Statement statement = connection.createStatement()) {

        	
            
        	String line;
            
        	boolean row_one = true;
            
        	StringBuilder queryBuilder = new StringBuilder(Insertionn);
            
        	int col_countt = columns.split(",").length;

            
        	while ((line = br.readLine()) != null) {
            
        		if (row_one) {
                    
        			
        			row_one = false;
                
                    continue;
                }

                
                List<String> val = parseCSVLine(line);

                if (val.size() != col_countt) {
                    System.out.println("Skipping malformed row: " + line);
                    continue; 
                }

                
                if (tableName.equals("products")) {
                    val.set(2, val.get(2).replace("$", "")); 
                }

                queryBuilder.append("(");
                
                for (String value : val) {
                
                	queryBuilder.append("'" + value.trim().replace("'", "\\'") + "',");
                
                
                }
                

                queryBuilder.setLength(queryBuilder.length() - 1);
                
                queryBuilder.append("),");
                
            }

            if (queryBuilder.length() > Insertionn.length()) {
                
            	
            	queryBuilder.setLength(queryBuilder.length() - 1); 
                
                
                statement.executeUpdate(queryBuilder.toString());
                
                System.out.println("Data loaded for table: " + tableName);
                
            } 
            else {
            	
                System.out.println("No data found in file: " + fileName);
            }

        } catch (Exception e) {
        	
            e.printStackTrace();
        }
    }

    
    
    
    private static List<String> parseCSVLine(String line) {
    	
    	
        List<String> result = new ArrayList<>();
        
        boolean insideQuote = false;
        
        StringBuilder currentField = new StringBuilder();

        
        for (char c : line.toCharArray()) {
        
        	if (c == '"') {
            
        		insideQuote = !insideQuote;
        		
            } else if (c == ',' && !insideQuote) {
            	
            	
                result.add(currentField.toString().trim()); 
                
                currentField = new StringBuilder(); 
                
            } 
            else {
            	
                currentField.append(c); 
            }
        }
        
        result.add(currentField.toString().trim());

        return result;
    }
    
    
    
    
    
    
    
    
    
    
    private static void Populate_Product_Dimension(Connection connection, String fileName) {
    	
        String Pathh = "/Users/apple.store.pk/Desktop/University/Warehouse/" + fileName;
        
        String Insertionn = "INSERT IGNORE INTO products_dim (product_id, product_name, product_price, supplier_id, supplier_name, store_id, store_name) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (BufferedReader br = new BufferedReader(new FileReader(Pathh));
        
        		PreparedStatement preparedStatement = connection.prepareStatement(Insertionn)) {

            String line;
            
            boolean row_one = true;

            while ((line = br.readLine()) != null) {

                if (row_one) {
            
                	row_one = false;
                    continue;
                }

               
                String[] val = parseCSVLinee(line);

                if (val.length != 7) {
                	
                    
                	System.out.println("Skipping malformed row: " + line);
                    
                	continue;
                }

                preparedStatement.setInt(1, Integer.parseInt(val[0].trim())); 
                
                
                preparedStatement.setString(2, val[1].trim());
                
                preparedStatement.setDouble(3, Double.parseDouble(val[2].replace("$", "").trim())); 
                
                preparedStatement.setInt(4, Integer.parseInt(val[3].trim())); 
                
                preparedStatement.setString(5, val[4].trim()); 
                
                preparedStatement.setInt(6, Integer.parseInt(val[5].trim())); 
                
                preparedStatement.setString(7, val[6].trim()); 

                
                preparedStatement.addBatch();
                
            }

            preparedStatement.executeBatch();
            
            System.out.println("Data loaded into products_dim.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    
    
    
    
    
    
    private static String[] parseCSVLinee(String line) {
    	
    	
        List<String> result = new ArrayList<>();
        
        boolean insideQuote = false;
        
        StringBuilder currentField = new StringBuilder();

       
        for (char c : line.toCharArray()) {
        
        	if (c == '"') {
            
        		insideQuote = !insideQuote; 
            } 
        	
        	else if (c == ',' && !insideQuote) {
            
        		result.add(currentField.toString().trim()); 
                
        		currentField = new StringBuilder(); 
        		
            } else {
            	
                currentField.append(c);
                
            }
        }
        
        result.add(currentField.toString().trim());

        return result.toArray(new String[0]); 
    }
    
    
    
    
    
    

    private static void Populate_Customers_Dimension(Connection connection, String fileName) {
    	
    	
        String Pathh = "/Users/apple.store.pk/Desktop/University/Warehouse/" + fileName;
        
        String Insertionn = "INSERT IGNORE INTO customers_dim (customer_id, customer_name, gender) VALUES (?, ?, ?)";

        
        
        try (BufferedReader br = new BufferedReader(new FileReader(Pathh));
        
        
        		PreparedStatement preparedStatement = connection.prepareStatement(Insertionn)) {

        	
            
        	String line;
            
        	
            
        	boolean row_one = true;

        	
            
        	while ((line = br.readLine()) != null) {
            
        		
            	
        		if (row_one) {
                
        			
            		
        			row_one = false; 
                    
        			
            		continue;
                }
        		

        		
        		
        		
                String[] val = line.split(",");
                
                if (val.length != 3) {
                
                	System.out.println("Skipping malformed row ");
                    
                	continue;
                }

                preparedStatement.setInt(1, Integer.parseInt(val[0].trim())); 
                
                
                preparedStatement.setString(2, val[1].trim()); 
                
                preparedStatement.setString(3, val[2].trim());

                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            
            System.out.println("Data loaded into customers_dim.");

        } catch (Exception e) {
        	
        	
            e.printStackTrace();
            
            
        }
    }

    
    
    
    
    
    
    private static void Populate_Time_Dimension(Connection connection, String fileName) {
    	
        
    	String Pathh = "/Users/apple.store.pk/Desktop/University/Warehouse/" + fileName;
        
    	String Insertionn = "INSERT INTO time_dim (time_id, date, year, month, day, week_day, hour) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE date = VALUES(date)"; 

        try (BufferedReader br = new BufferedReader(new FileReader(Pathh));
        		
             PreparedStatement preparedStatement = connection.prepareStatement(Insertionn)) {

            String line;
            
            
            boolean row_one = true;

            while ((line = br.readLine()) != null) {
            
            	if (row_one) {
            		
                    row_one = false; 
                    
                    continue;
                }

            	
                String[] val = line.split(",");
                
                if (val.length < 6) { 
                
                	System.out.println("Skipping malformed row: " + line);
                    
                	continue;
                }

                int id_time = Integer.parseInt(val[5].trim());
                
                Timestamp timestamp = Timestamp.valueOf(val[1].trim()); 

                preparedStatement.setInt(1, id_time);
                
                preparedStatement.setDate(2, new java.sql.Date(timestamp.getTime())); 
                
                preparedStatement.setInt(3, timestamp.toLocalDateTime().getYear()); 
                
                preparedStatement.setInt(4, timestamp.toLocalDateTime().getMonthValue()); 
                
                preparedStatement.setInt(5, timestamp.toLocalDateTime().getDayOfMonth()); 
                
                preparedStatement.setString(6, timestamp.toLocalDateTime().getDayOfWeek().toString()); 
                
                preparedStatement.setInt(7, timestamp.toLocalDateTime().getHour()); 

                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            System.out.println("Data loaded into time_dim.");

        } catch (Exception e) {
        	
            e.printStackTrace();
        }
    }
    
    
    
    
    
    
    
    
    
    
    
    private static List<Transaction> extractt_transc(Connection connection) throws SQLException {
    
    	List<Transaction> transactions = new ArrayList<>();
        
    	String query = "SELECT * FROM transactions LIMIT ?";
        
    	try (PreparedStatement stmt = connection.prepareStatement(query)) {
        
    		stmt.setInt(1, chunkk);
            
    		ResultSet rs = stmt.executeQuery();
            
    		while (rs.next()) {
    			
                transactions.add(new Transaction(rs.getInt("order_id"), rs.getInt("product_id"), rs.getInt("customer_id"),
            
                		rs.getInt("quantity_ordered"), rs.getInt("time_id"), rs.getTimestamp("order_date")));
            }
        }
    	
        return transactions;
    }
    
    
    
    
    
    
    
    
    
    
    
    
    private static List<Product> loadProducts(Connection connection) throws SQLException {
    
    	List<Product> products = new ArrayList<>();
        
    	String query = "SELECT * FROM products_dim";
        
    	try (Statement stmt = connection.createStatement()) {
        
        	ResultSet rs = stmt.executeQuery(query);
            
        	while (rs.next()) {
            
        		products.add(new Product(rs.getInt("product_id"), rs.getString("product_name"), rs.getDouble("product_price"),
                
        				rs.getInt("supplier_id"), rs.getString("supplier_name"), rs.getInt("store_id"), rs.getString("store_name")));
            }
        	
        }
    	
        return products;
    }

    
    
    
    
    private static List<Customer> customer_loading(Connection connection) throws SQLException {
    
    	List<Customer> customers = new ArrayList<>();
        
    	String query = "SELECT * FROM customers_dim";
        
    	try (Statement stmt = connection.createStatement()) {
        
    		ResultSet rs = stmt.executeQuery(query);
            
    		while (rs.next()) {
            
    			customers.add(new Customer(rs.getInt("customer_id"), rs.getString("customer_name"), rs.getString("gender")));
            }
    		
        }
    	
        return customers;
    }
    
   
    
    
    

    private static List<Time> loading_timee(Connection connection) throws SQLException {
    	
    	
        List<Time> times = new ArrayList<>();
        
        String query = "SELECT * FROM time_dim";
        
        try (Statement stmt = connection.createStatement()) {
        
        	ResultSet rs = stmt.executeQuery(query);
            
        	while (rs.next()) {
            
        		times.add(new Time(rs.getInt("time_id"), rs.getDate("date"), rs.getInt("year"), rs.getInt("month"),
                
        				rs.getInt("day"), rs.getString("week_day"), rs.getInt("hour")));
            }
        	
        	
        }
        
        
        return times;
    }
    
    
    
    
    
    
    
    
    

   
    private static List<SalesFact> joinn_enrich(List<Transaction> transactions, List<Customer> customers, List<Product> products, List<Time> times)
    
    {
        List<SalesFact> salesFacts = new ArrayList<>();
    
        for (Transaction transaction : transactions) {
        
        	Product product = findProduct(transaction.getProductId(), products);
            
        	Customer customer = findCustomer(transaction.getCustomerId(), customers);
            
        	Time time = findTime(transaction.getTimeId(), times);

        	
            
        	if (product != null && customer != null && time != null) {
            
        		double totalSale = transaction.getQuantityOrdered() * product.getProductPrice();
                
        		SalesFact salesFact = new SalesFact(transaction.getOrderId(), transaction.getOrderDate(), product.getProductId(),
                
        				customer.getCustomerId(), time.getTimeId(), transaction.getQuantityOrdered(), totalSale);
        		
        		
                salesFacts.add(salesFact);
            }
        	
        }
        
        return salesFacts;
    }

    
    
    
    
    private static Product findProduct(int productId, List<Product> products) {
    	
        for (Product product : products) {
        
        	if (product.getProductId() == productId) {
            
        		return product;
            }
        	
        }
        
        return null;
    }

    
    
    
    
    
    
    
    
    private static Customer findCustomer(int customerId, List<Customer> customers) {
    	
        for (Customer customer : customers) {
        
        	if (customer.getCustomerId() == customerId) {
            
        		return customer;
            }
        }
        
        return null;
    }
    
    
    
    

    private static Time findTime(int id_time, List<Time> times) {
    
    	for (Time time : times) {
        
    		if (time.getTimeId() == id_time) {
            
    			return time;
            }
        }
    	
        return null;
    }
    
    
    
    

   
    
 private static void inserrt_Fact(Connection connection, List<SalesFact> salesFacts) throws SQLException {
	 
	 
        String checkkk = "SELECT COUNT(*) FROM sales_fact WHERE order_id = ?";
     
        String Insertionn = "INSERT INTO sales_fact (order_id, order_date, product_id, customer_id, time_id, quantity_ordered, total_sale) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        
        
        try (PreparedStatement checkStmt = connection.prepareStatement(checkkk);
        
        		PreparedStatement insertStmt = connection.prepareStatement(Insertionn)) {

        	
            for (SalesFact salesFact : salesFacts) {
            
            	
            	
            	checkStmt.setInt(1, salesFact.getOrderId());
                
                ResultSet rs = checkStmt.executeQuery();
                
                rs.next();
                
                int count = rs.getInt(1);

               
                if (count == 0) {
                	
                    insertStmt.setInt(1, salesFact.getOrderId());
                    
                    insertStmt.setTimestamp(2, salesFact.getOrderDate());
                    
                    insertStmt.setInt(3, salesFact.getProductId());
                    
                    insertStmt.setInt(4, salesFact.getCustomerId());
                    
                    insertStmt.setInt(5, salesFact.getTimeId());
                    
                    insertStmt.setInt(6, salesFact.getQuantityOrdered());
                    
                    insertStmt.setDouble(7, salesFact.getTotalSale());
                    
                    insertStmt.addBatch();
                }
            }
            
            insertStmt.executeBatch();
        }
    }
    
    
    
    
    
    
    
    
    
  
    static class Transaction {
    	
        private int orderId;
        private int productId;
        private int customerId;
        private int quantityOrdered;
        private int id_time;
        private Timestamp orderDate;
        
        

        public Transaction(int orderId, int productId, int customerId, int quantityOrdered, int id_time, Timestamp orderDate) {
          
        	
        	this.orderId = orderId;
            this.productId = productId;
            this.customerId = customerId;
            this.quantityOrdered = quantityOrdered;
            this.id_time = id_time;
            this.orderDate = orderDate;
            
            
        }

        public int getOrderId() { 
        	
        	return orderId; 
        	
        }
        
        public int getProductId() {
        	
        	return productId; 
        	
        }
        
        public int getCustomerId() {

        	return customerId;
        	
        }
        
        public int getQuantityOrdered() { 
        	
        	return quantityOrdered; 
        	
        }
        
        public int getTimeId() { 
        	
        	return id_time; 
        	
        }
        
        
        public Timestamp getOrderDate() {
        	
        	return orderDate; 
        	}
        
        
    }

    
    
    
    
    
    
    
    
    
   
    static class Product {
    	
    	
        private int productId;
        private String productName;
        private double productPrice;
        private int supplierId;
        private String supplierName;
        private int storeId;
        private String storeName;

        public Product(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId, String storeName) {
          
        	
        	
        	this.productId = productId;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.storeId = storeId;
            this.storeName = storeName;
            
            
            
            
        }

        public int getProductId() { 
        
        	return productId; 
        	
        }
        
        
        
        public String getProductName() { 
        
        	
        	return productName; 
        	
        	
        }
        
        public double getProductPrice() { 
        	
        	
        	return productPrice; 
        	
        
        }
        
        
        public int getSupplierId() { 
        	
        	
        	return supplierId; 
        	
        }
        
        
        public String getSupplierName() { 
        	
        	
        	return supplierName; 
        	
        }
        
        public int getStoreId() {
        	
        	return storeId; 
        	
        }
        
        public String getStoreName() {
        	
        	return storeName; 
        	
        }
        
        
        
    }

    
    
    
    
    
    
    
    
    
    
    
    static class Customer {
    	
        private int customerId;
        
        private String customerName;
        
        private String gender;

        public Customer(int customerId, String customerName, String gender) {
        
        	this.customerId = customerId;
        	
            this.customerName = customerName;
            
            this.gender = gender;
       
        
        }
        
        

        public int getCustomerId() { 
        	
        	return customerId; 
        	
        }
        
        
        public String getCustomerName() {
        	
        	
        	return customerName; 
        	
        }
        
        
        
        public String getGender() { 
        	
        	
        	return gender;
        	
        }
    
    
    }
    
    
    
    
    
    
    
    
    
    

    
    static class Time {
    	
    	
    	
        private int id_time;
        private Date date;
        private int year;
        private int month;
        private int day;
        private String weekDay;
        private int hour;

        public Time(int id_time, Date date, int year, int month, int day, String weekDay, int hour)
        
        {
        	
        	
        	
            this.id_time = id_time;
            this.date = date;
            this.year = year;
            this.month = month;
            this.day = day;
            this.weekDay = weekDay;
            this.hour = hour;
            
            
            
            
        }

        
        
        
        public int getTimeId() {
        	
        	return id_time; 
        	
        	
        	}
        
        public Date getDate() { 
        	
        	
        	return date; 
        	
        
        }
        
        
        public int getYear() {
        	
        	
        	return year; 
        	
        }
        
        
        public int getMonth() {
        	
        	return month; 
        	
        }
        
        
        public int getDay() { 
        	
        	return day;
        	
        
        }
        
        public String getWeekDay() { 
        	
        	return weekDay; 
        	
        }
        
        
        public int getHour() {
        	
        	return hour; 
        	
        }
    
    
    
    
    }

    
    
    
    
    
       static class SalesFact {
    	
    	
        private int orderId;
        private Timestamp orderDate;
        private int productId;
        private int customerId;
        private int id_time;
        private int quantityOrdered;
        private double totalSale;

        public SalesFact(int orderId, Timestamp orderDate, int productId, int customerId, int id_time, int quantityOrdered, double totalSale) {
           
        	
        	this.orderId = orderId;
            this.orderDate = orderDate;
            this.productId = productId;
            this.customerId = customerId;
            this.id_time = id_time;
            this.quantityOrdered = quantityOrdered;
            this.totalSale = totalSale;
            
            
        }
        
        

        public int getOrderId() {
        	
        	return orderId; 
        	
        }
        
        
        public Timestamp getOrderDate() { 
        	
        	return orderDate; 
        	
        }
        
        
        
        public int getProductId() {
        	
        	return productId; 
        	
        }
        
        
        
        public int getCustomerId() { 
        	
        	
        	return customerId; 
        	
        	
        }
        
        
        
        public int getTimeId() {
        	
        	
        	
        	return id_time; 
        	
        }
        
        
        
        
        
        public int getQuantityOrdered() { 
        	
        	return quantityOrdered; 
        	
        }
        
        public double getTotalSale() { 
        	
        	return totalSale; 
        	
        }
        
        
        
        
    }
}
    
    
    
    
    



