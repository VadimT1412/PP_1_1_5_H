package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static jm.task.core.jdbc.util.Util.getConnection;

public class UserDaoJDBCImpl implements UserDao {
    private String createUsersTable = "CREATE TABLE IF NOT EXISTS USER (ID BIGINT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(45), LASTNAME VARCHAR(45), AGE TINYINT)";
    private String dropUsersTable = "DROP TABLE IF EXISTS USER";
    private String saveUser = "INSERT INTO USER(NAME, LASTNAME, AGE) VALUES(?,?,?)";
    private String removeUserById = "DELETE FROM USER WHERE ID=?";
    private String getAllUsers = "SELECT ID, NAME, LASTNAME, AGE FROM USER";
    private String cleanUsersTable = "TRUNCATE TABLE USER";

    public UserDaoJDBCImpl() {

    }

    private Connection connection = getConnection();

    public void createUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate(createUsersTable);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(dropUsersTable);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(saveUser)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.execute();
            System.out.println("User с именем – " + name + " добавлен в базу данных");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(removeUserById)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> addAllUsers = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(getAllUsers);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                addAllUsers.add(user);
                System.out.println(user);
            }
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
        return addAllUsers;
    }

    public void cleanUsersTable() {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(cleanUsersTable);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }
}
