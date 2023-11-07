package org.example;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    public static void insertPerson(String person) throws SQLException {
        Connection conn = connect();
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO person(name) VALUES (?)");
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
                System.out.println("Without connection");
            }
        }
    }

    public static  void cleanDatabase() throws SQLException {
        Connection conn = connect();
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("DROP TABLE IF EXISTS person");
            preparedStatement.executeUpdate();
            preparedStatement = conn.prepareStatement("CREATE TABLE person(id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)");
            preparedStatement.executeUpdate();
            preparedStatement.close();
            System.out.println("DB initialized");
            conn.close();
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void countRows() throws SQLException {
        Connection conn = connect();
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(*) AS recordCount from person");
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            int count = rs.getInt("recordCount");
            preparedStatement.close();
            rs.close();
            System.out.println("DB rows count: " + count);
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }else {
                System.out.println("Without connection");
            }
        }
    }

    public static void getFirstRecord() throws SQLException {
        Connection conn = connect();
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT name FROM person ORDER BY id LIMIT 1");
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            String name = rs.getString("name");
            preparedStatement.close();
            rs.close();
            System.out.println("First db row: " + name);
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }else {
                System.out.println("Without connection");
            }
        }
    }

    public static void getLastRecord() throws SQLException {
        Connection conn = connect();
        try  {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT name FROM person ORDER BY id DESC LIMIT 1");
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            String name = rs.getString("name");
            preparedStatement.close();
            rs.close();
            System.out.println("Last db row: " + name);
        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }else {
                System.out.println("Without connection");
            }
        }
    }
}

