package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static jm.task.core.jdbc.util.Util.getConnection;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    private Connection connection = getConnection();

    public void createUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS USER (ID BIGINT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(45), LASTNAME VARCHAR(45), AGE INT)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute("DROP TABLE IF EXISTS USER");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO USER(NAME, LASTNAME, AGE) VALUES(?,?,?)")) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.execute();
            System.out.println("User с именем – " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM USER WHERE ID=?")) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> addAllUsers = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT ID, NAME, LASTNAME, AGE FROM USER");
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                addAllUsers.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(addAllUsers);
        return addAllUsers;
    }

    public void cleanUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute("TRUNCATE TABLE USER");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
