package io.github.whoisalphahelix.sql;

import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.Getter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class SQLTable<T> {

    private static final String INSERT = "INSERT INTO %s (%s) VALUES (%s)";
    private static final String SELECT_ALL = "SELECT * FROM %s";
    private static final String SELECT_WHERE = SELECT_ALL + " WHERE %s = %s";
    private static final String SELECT_WHERE_MULTIPLE = SELECT_ALL + " WHERE %s";
    private static final String SELECT_COLUMN = "SELECT %s FROM %s";
    private static final String REMOVE = "DELETE FROM %s WHERE %s = %s";
    private static final String REMOVE_WHERE = "DELETE FROM %s WHERE %s";
    private static final String DELETE = "DELETE FROM %s";
    private static final String UPDATE = "UPDATE %s SET %s = %s WHERE %s = %s";
    private static final String UPDATE_WHERE = "UPDATE %s SET %s = %s WHERE %s";
    private static final JsonHelper JSON = new JsonHelper();

    private final SQL sql;
    private final String tableName;
    private final String tableInfo;
    private final SQLColumn[] sqlColumns;
    private final Map<String, Object> keyValueStore = new HashMap<>();
    private final Function<List<?>, T> mapper;

    SQLTable(SQL sql, String tableName, SQLColumn[] columns, Function<List<?>, T> mapper) {
        this.sql = sql;
        this.sqlColumns = columns;

        StringBuilder tableInfoBuilder = new StringBuilder();

        for (SQLColumn c : sqlColumns)
            tableInfoBuilder.append(",").append(c.getName());

        this.tableName = tableName;
        this.tableInfo = tableInfoBuilder.replace(0, 1, "").toString();
        this.mapper = mapper;
    }

    public SQLTable<T> insert(SQLColumn[] columns, Object... values) {
        return insert(Arrays.stream(columns).map(SQLColumn::getName).toArray(String[]::new),
                Arrays.stream(values).map(this::setEscaped).toArray());
    }

    public SQLTable<T> insert(String[] columns, Object... values) {
        if (values.length != columns.length)
            return this;

        StringBuilder columnInfo = new StringBuilder();

        for (String column : columns)
            columnInfo.append(",").append(column);

        StringBuilder sqlValues = new StringBuilder();

        for (Object str : values)
            sqlValues.append(",").append(str);

        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(INSERT, tableName, columnInfo.replace(0, 1, "").toString(),
                sqlValues.toString().replaceFirst(",", ""));

        eUpdate(query);

        return this;
    }

    public SQLTable<T> insert(Object... values) {
        return insert(getSqlColumns(), values);
    }

    public SQLTable<T> insert(T o) {
        if (!o.getClass().isAnnotationPresent(Table.class)) return null;

        return insert(demap(o));
    }

    public SQLTable<T> insert(String column, String data) {
        String val = "'" + data + "'";

        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(INSERT, tableName, column, val);

        eUpdate(query);

        return this;
    }

    public SQLTable<T> insert(String column, Object data) {
        return insert(column, set(data));
    }

    public SQLTable<T> insert(SQLColumn column, String data) {
        return insert(column.getName(), data);
    }

    public SQLTable<T> insert(SQLColumn column, Object data) {
        return insert(column.getName(), data);
    }

    public SQLTable<T> insertIf(String column, Object data, BiFunction<String, Object, Boolean> condition) {
        return condition.apply(column, data) ? insert(column, data) : this;
    }

    public SQLTable<T> insertIfAbsent(String column, Object data) {
        return insertIf(column, data, this::contains);
    }

    public SQLTable<T> insertIf(T data, Function<T, Boolean> condition) {
        return condition.apply(data) ? insert(data) : this;
    }

    public SQLTable<T> insertIfAbsent(T data) {
        return insertIf(data, this::contains);
    }

    public T getRow(SQLKey<?> key) {
        return mapper.apply(getRowData(key));
    }

    public T getRow(SQLKey<?>... keys) {
        return mapper.apply(getRowData(keys));
    }

    public T getRow(SQLColumn column, Object key) {
        return mapper.apply(getRowData(column, key));
    }

    public T getRow(SQLColumn[] columns, Object... keys) {
        return mapper.apply(getRowData(columns, keys));
    }

    public T getRow(String column, Object key) {
        return mapper.apply(getRowData(column, key));
    }

    public T getRow(String[] columns, Object... keys) {
        return mapper.apply(getRowData(columns, keys));
    }

    public List<?> getRowData(String[] columns, Object... keys) {
        if (columns.length != keys.length || !getSql().getDataSource().isRunning())
            return new LinkedList<>();

        String query = buildMultipleWhereQuery(columns, keys);

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prepstate = con.prepareStatement(query);
            prepstate.closeOnCompletion();

            return groupTo(prepstate.executeQuery());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    public List<?> getRowData(SQLKey<?>... keys) {
        return getRowData(Arrays.stream(keys).map(SQLKey::getColumn).toArray(SQLColumn[]::new),
                Arrays.stream(keys).map(SQLKey::getKey).toArray());
    }

    public List<?> getRowData(SQLColumn[] columns, Object... keys) {
        return getRowData(Arrays.stream(columns).map(SQLColumn::getName).toArray(String[]::new), keys);
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

        String query = String.format(SELECT_WHERE, tableName, column, setEscaped(key));

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prepstate = con.prepareStatement(query);
            prepstate.closeOnCompletion();

            return groupTo(prepstate.executeQuery());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    public List<T> getAll() {
        return getAllData().stream().map(mapper).collect(Collectors.toList());
    }

    public List<List<?>> getAllData() {
        if (!getSql().getDataSource().isRunning())
            return new LinkedList<>();

        List<List<?>> objs = new LinkedList<>();
        String query = String.format(SELECT_ALL, tableName);

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            ResultSet rs = prep.executeQuery();

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

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            ResultSet rs = prep.executeQuery();

            while (rs.next())
                objs.add(getJson(rs.getString(column)));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return objs;
    }

    public List<List<?>> getAllColumnData(String... column) {
        return Arrays.stream(column).map(this::getAllColumnData).collect(Collectors.toList());
    }

    public List<?> getData(SQLKey<?>... keys) {
        return getData(Arrays.stream(keys).map(SQLKey::getColumn).toArray(SQLColumn[]::new),
                Arrays.stream(keys).map(SQLKey::getKey).toArray());
    }

    public List<?> getData(SQLColumn[] columns, Object... keys) {
        return getData(Arrays.stream(columns).map(SQLColumn::getName).toArray(String[]::new), keys);
    }

    public List<?> getData(String[] columns, Object... keys) {
        if (columns.length != keys.length)
            return new LinkedList<>();

        List<?> data = new LinkedList<>();

        for (int i = 0; i < columns.length; i++)
            data.add(getData(columns[i], keys[i]));

        return data;
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

        String query = String.format(SELECT_WHERE, tableName, column, setEscaped(key));

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prepstate = con.prepareStatement(query);
            prepstate.closeOnCompletion();

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

        String query = String.format(REMOVE, tableName, column, setEscaped(key));

        return executeQueryUpdate(query);
    }

    public SQLTable<T> remove(SQLKey<?>... keys) {
        return remove(Arrays.stream(keys).map(SQLKey::getColumn).toArray(SQLColumn[]::new),
                Arrays.stream(keys).map(SQLKey::getKey).toArray());
    }

    public SQLTable<T> remove(SQLColumn[] columns, Object... keys) {
        return remove(Arrays.stream(columns).map(SQLColumn::getName).toArray(String[]::new), keys);
    }

    public SQLTable<T> remove(String[] columns, Object... keys) {
        if (columns.length != keys.length || !getSql().getDataSource().isRunning())
            return null;

        String query = String.format(REMOVE_WHERE, tableName, buildMultipleWhere(columns, keys));

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            prep.executeUpdate();
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
            return this;

        String query = String.format(UPDATE, tableName, valueColumn, setEscaped(value), checkColumn, setEscaped(key));

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this;
    }

    public SQLTable<T> update(SQLKey<?>[] keys, SQLKey<?> value) {
        return update(Arrays.stream(keys).map(SQLKey::getColumn).toArray(SQLColumn[]::new),
                Arrays.stream(keys).map(SQLKey::getKey).toArray(), value.getColumn(), value.getKey());
    }

    public SQLTable<T> update(SQLColumn[] keyColumns, Object[] keys, SQLColumn updateColumns, Object updateValue) {
        return update(Arrays.stream(keyColumns).map(SQLColumn::getName).toArray(String[]::new),
                keys, updateColumns.getName(), updateValue);
    }

    public SQLTable<T> update(String[] whereColumns, Object[] whereKeys, String updateColumn, Object updateValue) {
        if (whereColumns.length != whereKeys.length || !getSql().getDataSource().isRunning())
            return this;

        String query = String.format(UPDATE_WHERE, tableName, updateColumn, setEscaped(updateValue), buildMultipleWhere(whereColumns, whereKeys));

        return executeQueryUpdate(query);
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

        String query = String.format(SELECT_WHERE.replace("*", column), tableName, column, setEscaped(key));

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prepstate = con.prepareStatement(query);
            prepstate.closeOnCompletion();

            ResultSet rs = prepstate.executeQuery();

            return rs != null && rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean contains(SQLKey<?>... keys) {
        return contains(Arrays.stream(keys).map(SQLKey::getColumn).toArray(SQLColumn[]::new),
                Arrays.stream(keys).map(SQLKey::getKey).toArray());
    }

    public boolean contains(SQLColumn[] column, Object... keys) {
        return contains(Arrays.stream(column).map(SQLColumn::getName).toArray(String[]::new), keys);
    }

    public boolean contains(String[] columns, Object... keys) {
        if (columns.length != keys.length || !getSql().getDataSource().isRunning())
            return false;

        String query = buildMultipleWhereQuery(columns, keys);

        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prepstate = con.prepareStatement(query);
            prepstate.closeOnCompletion();

            ResultSet rs = prepstate.executeQuery();

            return rs != null && rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean contains(T data) {
        return contains(getSqlColumns(), demap(data));
    }

    public SQLTable<T> empty() {
        if (!getSql().getDataSource().isRunning())
            return null;

        String query = String.format(DELETE, tableName);

        eUpdate(query);
        return this;
    }

    public Optional<SQLColumn> columnByName(String column) {
        return Arrays.stream(getSqlColumns()).filter(column1 -> column1.getName().equals(column)).findFirst();
    }

    public Stream<SQLColumn> columnByType(String type) {
        return Arrays.stream(getSqlColumns()).filter(column -> column.getType().equals(type));
    }

    public Object[] demap(T data) {
        return this.sql.getColumnFields(data.getClass()).map(saveField -> {
            try {
                return saveField.get(data);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return null;
        }).filter(Objects::nonNull).toArray();
    }

    private String buildMultipleWhereQuery(String[] columns, Object... keys) {
        return String.format(SELECT_WHERE_MULTIPLE, tableName, buildMultipleWhere(columns, keys));
    }

    private String buildMultipleWhere(String[] columns, Object... keys) {
        StringBuilder where = new StringBuilder();

        for (int i = 0; i < columns.length; i++)
            where.append(columns[i]).append(" = ").append(setEscaped(keys[i])).append(" AND ");

        where = where.reverse().replace(0, 4, "").reverse();

        return where.toString();
    }

    private SQLTable<T> executeQueryUpdate(String query) {
        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return this;
    }

    private <G> G getJson(String rs) {
        return (G) JSON.fromJsonTree(JsonHelper.gson(), rs);
    }

    private String setEscaped(Object o) {
        StringBuilder str = new StringBuilder(set(o));

        if (str.toString().startsWith("\"") && str.toString().endsWith("\""))
            return "\'" + str.replace(0, 1, "").reverse().replace(0, 1, "").reverse().toString()
                    .replace("\"", "\\\"") + "\'";

        return "\'" + str.toString() + "\'";
    }

    private String set(Object o) {
        return JSON.toJsonTreeString(JsonHelper.gson(), o);
    }

    private void eUpdate(String query) {
        try (Connection con = getSql().getDataSource().getConnection()) {
            PreparedStatement prep = con.prepareStatement(query);
            prep.closeOnCompletion();
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<?> groupTo(ResultSet rs) {
        if (rs == null)
            return null;

        try {
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
}
