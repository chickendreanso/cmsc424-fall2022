import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;

public class NearestNeighbor {
	static double jaccard(HashSet<String> s1, HashSet<String> s2) {
		int total_size = s1.size() + s2.size();
		int i_size = 0;
		for (String s : s1) {
			if (s2.contains(s))
				i_size++;
		}
		return ((double) i_size) / (total_size - i_size);
	}

	public static void executeNearestNeighbor() {
		/*************
		 * Add your code to add a new column to the users table (set to null by
		 * default), calculate the nearest neighbor for each node (within first 5000),
		 * and write it back into the database for those users..
		 ************/

		// Load the PostgreSQL JDBC Driver
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");

		// Set up the connection
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange", "root", "root");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
			return;
		}

		// Run a query and print out the results by iterating through the resultset
		Statement stmt = null;
		String query = "alter table users add column nearest_neighbor integer default null;";
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String name = rs.getString("nearest_neighbor");
				System.out.println(name + "\t");
			}
			stmt.close();
		} catch (SQLException e) {
			System.out.println(e);
		}

		// Use the following query to fetch relevant data from the database:
		stmt = null;
		query = "select users.id, array_remove(array_agg(posts.tags), null) as arr from users, posts where users.id = posts.OwnerUserId and users.id < 5000 group by users.id having count(posts.tags) > 0;";
		HashMap<Integer, HashSet<String>> user_tags = new HashMap<Integer, HashSet<String>>();
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int id = rs.getInt("id");
				String tags = rs.getString("arr");
				// remove { and } and , from the string
				tags = tags.replaceAll("\\{", "");
				tags = tags.replaceAll("}", "");
				tags = tags.replaceAll(",", "");
				tags = tags.replaceAll("><", ",");
				tags = tags.replaceAll("<", "");
				tags = tags.replaceAll(">", "");
				HashSet<String> tag_set = new HashSet<String>();
				for (String tag : tags.split(",")) {
					tag_set.add(tag);
				}
				user_tags.put(id, tag_set);
			}
			stmt.close();
			// loop through the users and find the nearest neighbor

		} catch (SQLException e) {
			System.out.println(e);
		}

		// Use the following query to write the nearest neighbor back into the database:
		// 1. Loop through the users
		for (int user : user_tags.keySet()) {
			double max_jaccard = 0;
			int nearest_neighbor = -1;
			// 2. For each user, loop through the other users
			for (int other_user : user_tags.keySet()) {
				// 3. Calculate the Jaccard similarity between the two users
				if (user == other_user)
					continue;
				double jaccard = jaccard(user_tags.get(user), user_tags.get(other_user));
				if (jaccard > max_jaccard) {
					max_jaccard = jaccard;
					nearest_neighbor = other_user;
				} else if (jaccard == max_jaccard) {
					if (other_user < nearest_neighbor) {
						nearest_neighbor = other_user;
					}
				}
			}
			// update the nearest neighbor in the database
			String update_query = "update users set nearest_neighbor = " + nearest_neighbor + " where id = " + user
					+ ";";
			System.out.println(update_query);
			stmt = null;
			try {
				stmt = connection.createStatement();
				stmt.executeUpdate(update_query);
				stmt.close();
			} catch (SQLException e) {
				System.out.println(e);
			}
		}

		// test
		stmt = null;
		query = "select * from users order by id limit 10;";
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int id = rs.getInt("id");
				int nearest_neighbor = rs.getInt("nearest_neighbor");
				System.out.println(id + "\t" + nearest_neighbor);
			}
			stmt.close();
		} catch (SQLException e) {
			System.out.println(e);
		}

		return;
	}

	public static void main(String[] argv) {
		executeNearestNeighbor();
	}
}
