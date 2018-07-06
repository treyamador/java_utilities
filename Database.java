package art;


import java.util.Iterator;
import java.lang.Iterable;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


public class Database {
	
	
	private final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
	private Connection connect = null;
    
	
    public Database(String db, String user, String pwd) {
    	this.init(db, user, pwd);
    }
	
	
    private void init(String db, String user, String pwd) {
    	try {
			Class.forName(this.DRIVER_NAME);
			this.connect = DriverManager.getConnection(
					"jdbc:mysql://localhost/" + db + "?" + 
							"user=" + user + "&password=" + pwd +
							"&allowPublicKeyRetrieval=true&useSSL=false");
			// Statement statement = this.connect.createStatement();
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
    
    private ResultSetMetaData getMetaData(String table) {
    	ResultSetMetaData metadata = null;
    	Statement statement = null;
    	try {
    		statement = this.connect.createStatement();
			metadata = statement.executeQuery("SELECT * from "+table).getMetaData();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    	return metadata;
    }
    
    
	private String queryInsert(ResultSetMetaData metadata, String table) {
		String labels = "INSERT into " + table + " (";
		String places = "VALUES (";
		try {
			for (int i = 1; i <= metadata.getColumnCount(); ++i) {
				labels += metadata.getColumnName(i)+", ";
				places += "?, ";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		labels = labels.substring(0, labels.length()-2);
		places = places.substring(0, places.length()-2);
		return labels + ") " + places + ")";
	}
	
	
	public boolean insert(String table, Iterable<Object> elems) {
		
		try {			
			ResultSetMetaData metadata = this.getMetaData(table);
			String query = this.queryInsert(metadata, table);
			PreparedStatement preparedStatement = this.connect.prepareStatement(query);
			
			Iterator<Object> iter = elems.iterator();
			int len = metadata.getColumnCount();
			for (int i = 1; i <= len && iter.hasNext(); ++i) {
				preparedStatement.setObject(i, iter.next());
			}
			preparedStatement.execute();
		
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			return false;
		}
		
		return true;
		
	}
	
	
	public boolean insert(String table, String elem) {
		ArrayList<Object> elems = new ArrayList<Object>();
		elems.add(elem);
		return this.insert(table, elems);
	}
	
	
	private String selectQuery(String table, Iterable<Object> keys) {
		String query = "SELECT ";
		Iterator<Object> keyIter = keys.iterator();
		if (keyIter.hasNext()) {
			while (keyIter.hasNext()) {
				Object elemObj = keyIter.next();
				query += elemObj.toString() + ", ";
			}
			query = query.substring(0, query.length()-2);
		} else {
			query += "*";
		}
		query += " from " + table;
		return query;
	}
	
	
	public ResultSet select(String table) {
		ArrayList<Object> key = new ArrayList<Object>();
		return this.select(table, key);
	}
	
	
	public ResultSet select(String table, String column) {
		ArrayList<Object> key = new ArrayList<Object>();
    	key.add(column);
    	return this.select(table, key);    	
    }
    
    
	public ResultSet select(String table, Iterable<Object> keys) {
		String query = this.selectQuery(table, keys);
		ResultSet result = null;
    	Statement statement = null;
    	try {
			statement = this.connect.createStatement();
			result = statement.executeQuery(query);			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    	return result;
	}
	
		
	public boolean hasEntry(String table, String key, String val) {
		Statement statement = null;
		ResultSet result = null;
		try {
			String query = "SELECT " + key + " from " + table + " where " + key + " = '" + val+"'";
			statement = this.connect.createStatement();
			result = statement.executeQuery(query);
			return result.next() ? true : false;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	
	public void print(ResultSet result) {
		try {
			int len = result.getMetaData().getColumnCount();
			while (result.next()) {
				for (int i = 1; i <= len; ++i)
					System.out.print(result.getString(i)+"   ");
				System.out.println();
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	
	public void close() {
		try {
			if (this.connect != null) {
				this.connect.close();
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	
}

