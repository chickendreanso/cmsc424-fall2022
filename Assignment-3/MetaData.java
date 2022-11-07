import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

public class MetaData {
	static String dataTypeName(int i) {
		switch (i) {
			case java.sql.Types.INTEGER:
				return "Integer";
			case java.sql.Types.REAL:
				return "Real";
			case java.sql.Types.VARCHAR:
				return "Varchar";
			case java.sql.Types.TIMESTAMP:
				return "Timestamp";
			case java.sql.Types.DATE:
				return "Date";
		}
		return "Other";
	}

	public static void executeMetadata(String databaseName) {
		/*************
		 * Add you code to connect to the database and print out the metadta for the
		 * database databaseName.
		 ************/

		// Load the PostgreSQL JDBC Driver
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return;
		}

		// Set up the connection
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName, "root",
					"root");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
		} else {
			System.out.println("Failed to make connection!");
			return;
		}

		// JDBC also allows inspection of the schema, which can be a powerful feature to
		// explore new databases (or datasets). Here, your task is to use that
		// functionality to create a short summary of the tables in a database and the
		// possible joins between the tables of the database based on the foreign keys.
		// Specifically, you should use the getMetaData() function to obtain
		// DatabaseMetaData object, and use that to fetch information about the tables
		// in the database as well as information about the primary keys and foreign
		// keys.
		// This resource here has detailed examples of how to use this functionality:
		// https://www.baeldung.com/jdbc-database-metadata
		// We have provided a skeleton file, MetaData.java -- your task is to complete
		// the function within.
		// For the expected output format, see the files
		// exampleOutputMetadataStackexchange.txt and
		// exampleOutputMetadataUniversity.txt, which should be the output of running
		// the program on our stackexchange and university databases. The overall order
		// of tables and joinable relationships doesn't matter -- however, the lists of
		// attributes (for a table and for a primary key) should be sorted in the
		// increasing order (use Collections.sort()).
		// Notes/Hints:
		// Use .toUpperCase() to convert table/attribute names to uppper case.
		// We have provided a function to map the integer type that JDBC uses to a
		// String, that you can use when printing out the data type of a column.
		// Use Collections.sot() to sort the attribute lists.
		Statement stmt = null;
		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";
		HashSet<String> tables = new HashSet<String>();
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				tables.add(rs.getString("table_name"));
			}
			System.out.println("### Tables in the Database");
			for (String table : tables) {
				System.out.println("-- Table " + table.toUpperCase());
				// Attributes
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs1 = dbmd.getColumns(null, null, table, null);
				ArrayList<String> attributes = new ArrayList<String>();
				while (rs1.next()) {
					String columnName = rs1.getString("COLUMN_NAME").toUpperCase();
					String columnType = rs1.getString("DATA_TYPE");
					if (columnType.equals("2")) {
						columnType = "Numeric";
					} else if (columnType.equals("4")) {
						columnType = "Integer";
					} else if (columnType.equals("12")) {
						columnType = "Varchar";
					} else if (columnType.equals("91")) {
						columnType = "Date";
					}
					attributes.add(columnName + " (" + columnType + ")");
				}
				Collections.sort(attributes);
				System.out.print("Attributes: ");
				for (int i = 0; i < attributes.size(); i++) {
					System.out.print(attributes.get(i));
					if (i != attributes.size() - 1) {
						System.out.print(", ");
					}
				}
				System.out.println();
				// Primary Key
				ResultSet rs2 = dbmd.getPrimaryKeys(null, null, table);
				System.out.print("Primary Key: ");
				ArrayList<String> primaryKeys = new ArrayList<String>();
				while (rs2.next()) {
					String columnName = rs2.getString("COLUMN_NAME").toUpperCase();
					primaryKeys.add(columnName);
				}
				Collections.sort(primaryKeys);
				for (int i = 0; i < primaryKeys.size(); i++) {
					System.out.print(primaryKeys.get(i));
					if (i != primaryKeys.size() - 1) {
						System.out.print(", ");
					}
				}
				System.out.println();
			}
			System.out.println();
			System.out.println("### Joinable Pairs of Tables (based on Foreign Keys)");
			ArrayList<String> joinablePairs = new ArrayList<String>();
			for (String table : tables) {
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs3 = dbmd.getImportedKeys(null, null, table);
				while (rs3.next()) {
					String pkTableName = rs3.getString("PKTABLE_NAME").toUpperCase();
					String pkColumnName = rs3.getString("PKCOLUMN_NAME").toUpperCase();
					String fkTableName = rs3.getString("FKTABLE_NAME").toUpperCase();
					String fkColumnName = rs3.getString("FKCOLUMN_NAME").toUpperCase();
					joinablePairs.add(pkTableName + " can be joined " + fkTableName + " on attributes " + pkColumnName
							+ " and " + fkColumnName);
				}
			}
			joinablePairs.sort(null);
			for (String pair : joinablePairs) {
				System.out.println(pair);
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

	}

	public static void main(String[] argv) {
		executeMetadata(argv[0]);
	}
}
