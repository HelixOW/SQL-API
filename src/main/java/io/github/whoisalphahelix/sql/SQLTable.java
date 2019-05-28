package io.github.whoisalphahelix.sql;

import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.Getter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class SQLTable<T> {

    private static final String INSERT = "INSERT INTO %s (%s) VALUES (%s);";
    private static final String SELECT_ALL = "SELECT * FROM %s";
    private static final String SELECT = SELECT_ALL + " WHERE %s = ?";
    private static final String SELECT_COLUMN = "SELECT %s FROM %s";
    private static final String REMOVE = "DELETE FROM %s WHERE(%s = ?)";
    private static final String DELETE = "DELETE FROm %s";
    private static final String UPDATE = "UPDATE %s SET %s = ? WHERE %s=?";
    private static final JsonHelper JSON = new JsonHelper();

    private final SQL sql;
    private final String tableName;
    private final String tableInfo;
    private final SQLColumn[] sqlColumns;
    private final Map<String, Object> keyValueStore = new HashMap<>();

    SQLTable(SQL sql, String tableName, SQLColumn[] sqlColumns) {
        this.sql = sql;
        this.sqlColumns = sqlColumns;

        StringBuilder tableInfoBuilder = new StringBuilder();

        for (SQLColumn c : sqlColumns)
            tableInfoBuilder.append(",").append(c.getName());

        this.tableName = tableName;
        this.tableInfo = tableInfoBuilder.replace(0, 1, "").toString();
    }

    public SQLTable<T> insert(String... values) {
        StringBuilder sqlValues = new StringBuilder();
        for (String str : values)
            sqlValues.append(",").append("'").append(str).append("'");

        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(INSERT, tableName, getTableInfo(), sqlValues.toString().replaceFirst(",", ""));

        try {
            getSql().getDataSource().getConnection().prepareStatement(query).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return this;
    }

    public SQLTable<T> insert(T o) {
        if (!o.getClass().isAnnotationPresent(Table.class)) return null;

        String[] values = this.sql.getColumnFields(o.getClass()).map(saveField -> {
            if (saveField.getType().isPrimitive()) {
                try {
                    return saveField.get(o).toString();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    return set(saveField.get(o));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            return "";
        }).filter(s -> !s.isEmpty()).toArray(String[]::new);

        return insert(values);
    }

    public T getRow(SQLKey<?> key, Function<List<?>, T> mapper) {
        return mapper.apply(getRowData(key));
    }

    public T getRow(SQLColumn column, Object key, Function<List<?>, T> mapper) {
        return mapper.apply(getRowData(column, key));
    }

    public T getRow(String column, Object key, Function<List<?>, T> mapper) {
        return mapper.apply(getRowData(column, key));
    }

    public List<?> getRowData(SQLKey<?> key) {
        return getRowData(key.getColumn(), key.getKey());
    }

    public List<?> getRowData(SQLColumn column, Object key) {
        return getRowData(column.getName(), key);
    }

    public List<?> getRowData(String column, Object key) {
        if (!getSql().getDataSource().isRunning())
            return new LinkedList<>();

        String query = String.format(SELECT, tableName, column);

        try {
            PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
            prepstate.setString(1, set(key));

            ResultSet rs = prepstate.executeQuery();

            if (rs == null)
                return null;

            if (rs.next()) {
                List<?> rowObjects = new LinkedList<>();

                for (SQLColumn c : this.getSqlColumns())
                    rowObjects.add(getJson(rs.getString(c.getName())));

                return rowObjects;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    public List<T> getAll(Function<List<?>, T> mapper) {
        return getAll().stream().map(mapper).collect(Collectors.toList());
    }

    public List<List<?>> getAll() {
        if (!getSql().getDataSource().isRunning())
            return new LinkedList<>();

        List<List<?>> objs = new LinkedList<>();
        String query = String.format(SELECT_ALL, tableName);

        try {
            ResultSet rs = getSql().getDataSource().getConnection().prepareStatement(query).executeQuery();

            while (rs.next()) {
                List<?> rowObjects = new LinkedList<>();

                for (SQLColumn c : this.getSqlColumns())
                    rowObjects.add(getJson(rs.getString(c.getName())));

                objs.add(rowObjects);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return objs;
    }

    public <C> List<C> getAllColumnData(String column) {
        if (!getSql().getDataSource().isRunning())
            return new LinkedList<>();

        List<C> objs = new LinkedList<>();
        String query = String.format(SELECT_COLUMN, column, tableName);

        try {
            ResultSet rs = getSql().getDataSource().getConnection().prepareStatement(query).executeQuery();

            while (rs.next())
                objs.add(getJson(rs.getString(column)));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return objs;
    }

    public <C> C getData(SQLKey<?> key) {
        return getData(key.getColumn().getName(), key.getKey());
    }

    public <C> C getData(SQLColumn column, Object key) {
        return getData(column.getName(), key);
    }

    public <C> C getData(String column, Object key) {
        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(SELECT, tableName, column);

        try {
            PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
            prepstate.setString(1, set(key));

            ResultSet rs = prepstate.executeQuery();

            if (rs == null)
                return null;

            if (rs.next())
                return getJson(rs.getString(column));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public SQLTable<T> remove(SQLKey<?> key) {
        return remove(key.getColumn().getName(), key.getKey());
    }

    public SQLTable<T> remove(SQLColumn column, Object key) {
        return remove(column.getName(), key);
    }

    public SQLTable<T> remove(String column, Object key) {
        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(REMOVE, tableName, column);

        try {
            PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
            prepstate.setString(1, set(key));
            prepstate.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return this;
    }

    public SQLTable<T> update(SQLKey<?> key, SQLKey<?> value) {
        return update(key.getColumn().getName(), key.getKey(), value.getColumn().getName(), value.getKey());
    }

    public SQLTable<T> update(SQLColumn checkColumn, Object key, SQLColumn valueColumn, Object value) {
        return update(checkColumn.getName(), key, valueColumn.getName(), value);
    }

    public SQLTable<T> update(String checkColumn, Object key, String valueColumn, Object value) {
        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(UPDATE, tableName, valueColumn, checkColumn);

        try {
            PreparedStatement prep = getSql().getDataSource().getConnection().prepareStatement(query);
            prep.setString(1, set(value));
            prep.setString(2, set(key));
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this;
    }

    public boolean contains(SQLKey<?> key) {
        return contains(key.getColumn().getName(), key.getKey());
    }

    public boolean contains(SQLColumn column, Object key) {
        return contains(column.getName(), key);
    }

    public boolean contains(String column, Object key) {
        if (!getSql().getDataSource().isRunning())
            return false;

        String query = String.format(SELECT, tableName, column);

        try {
            PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
            prepstate.setString(1, set(key));

            ResultSet rs = prepstate.executeQuery();

            if (rs == null)
                return false;

            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public SQLTable<T> empty() {
        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(DELETE, tableName);

        try {
            getSql().getDataSource().getConnection().prepareStatement(query).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this;
    }

    public Optional<SQLColumn> columnByName(String column) {
        return Arrays.stream(getSqlColumns()).filter(column1 -> column1.getName().equals(column)).findFirst();
    }

    public Stream<SQLColumn> columnByType(String type) {
        return Arrays.stream(getSqlColumns()).filter(column -> column.getType().equals(type));
    }

    private <G> G getJson(String rs) {
        return (G) JSON.fromJsonTree(JsonHelper.gson(), rs);
    }

    private String set(Object o) {
        return JSON.toJsonTree(JsonHelper.gson(), o).toString();
    }
}
