package org.example;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DbConnection {
    public DbConnection() {
    }
    private static BasicDataSource ds = new BasicDataSource();
    private static final String url = "jdbc:postgresql://localhost/postgres";
    private static final String user = "postgres";
    private static final String password = "321";

    static {
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setMinIdle(5);
        ds.setMaxIdle(Runtime.getRuntime().availableProcessors());
        ds.setMaxOpenPreparedStatements(1000000);
    }
    public static Connection connect() throws SQLException {
        return ds.getConnection();
    }
    public static void insertPeople(String person, Connection conn) throws SQLException {
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO persons (name) VALUES (?)");
            preparedStatement.setString(1, person);
            preparedStatement.executeUpdate();
            preparedStatement.close();

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }else {
                System.out.println("Nem tem conexao nessabosta");
            }
        }
    }

    public static  void cleanDatabase(Connection conn) {
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("DROP TABLE IF EXISTS persons");
            preparedStatement.executeUpdate();
            preparedStatement = conn.prepareStatement("CREATE TABLE persons(id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)");
            preparedStatement.executeUpdate();
            preparedStatement.close();
            System.out.println("Database clean");
            conn.close();
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

