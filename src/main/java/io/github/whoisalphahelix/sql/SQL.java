package io.github.whoisalphahelix.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.whoisalphahelix.sql.annotations.Column;
import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@EqualsAndHashCode
@ToString
public class SQL {

    private static final String CREATE = "CREATE TABLE IF NOT EXISTS %s (%s);";
    private static final String DROP = "DROP TABLE %s";

    private final HikariDataSource dataSource;

    public SQL(String driver, String jdbcPath, String username, String password) {
        HikariConfig config = new HikariConfig();

        config.setDriverClassName(driver);
        config.setJdbcUrl(jdbcPath);

        if (!username.isEmpty())
            config.setUsername(username);
        if (!password.isEmpty())
            config.setPassword(password);

        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        this.dataSource = new HikariDataSource(config);
    }

    public SQL(String driver, String jdbcPath) {
        this(driver, jdbcPath, "", "");
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public <T> SQLTable<T> createTable(String table, Function<List<?>, T> mapper, SQLColumn... sqlColumns) {
        StringBuilder infoBuilder = new StringBuilder();

        for (SQLColumn SQLColumn : sqlColumns)
            infoBuilder.append(SQLColumn.toSQL()).append(",");

        String info = infoBuilder.reverse().replace(0, 1, "").reverse().toString();
        String query = String.format(CREATE, table, info);

        try {
            getDataSource().getConnection().prepareStatement(query).execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new SQLTable<>(this, table, sqlColumns, mapper);
    }

    public <T> SQLTable<T> createTable(String table, SQLColumn... sqlColumns) {
        return createTable(table, null, sqlColumns);
    }

    public <T> SQLTable<T> createTable(Class<T> table) {
        Table tab = table.getAnnotation(Table.class);

        if (tab == null) return null;

        String name = (tab.value().isEmpty() ? table.getSimpleName() : tab.value());

        return this.createTable(name, table);
    }

    public <T> SQLTable<T> createTable(Class<T> table, Function<List<?>, T> mapper) {
        Table tab = table.getAnnotation(Table.class);

        if (tab == null) return null;

        String name = (tab.value().isEmpty() ? table.getSimpleName() : tab.value());

        return this.createTable(name, table, mapper);
    }

    public <T> SQLTable<T> createTable(String name, Class<T> table) {
        return createTable(name, getSQLColumns(table).toArray(SQLColumn[]::new));
    }

    public <T> SQLTable<T> createTable(String name, Class<T> table, Function<List<?>, T> mapper) {
        return createTable(name, mapper, getSQLColumns(table).toArray(SQLColumn[]::new));
    }

    public void dropTable(String table) {
        try {
            getDataSource().getConnection().prepareStatement(String.format(DROP, table)).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String guessSQLColumnType(Class<?> type) {
        if (type.equals(int.class) || type.equals(Integer.class))
            return "INTEGER";
        else if (type.equals(double.class) || type.equals(Double.class))
            return "DOUBLE";
        else if (type.equals(float.class) || type.equals(Float.class))
            return "FLOAT";
        else if (type.equals(short.class) || type.equals(Short.class))
            return "SMALLINT";
        else if (type.equals(boolean.class) || type.equals(Boolean.class))
            return "BIT";
        else if (type.equals(long.class) || type.equals(Long.class))
            return "BIGINT";
        else if (type.equals(byte.class) || type.equals(Byte.class))
            return "TINYINT";
        else if (type.equals(String.class))
            return "VARCHAR";

        return "JSON";
    }

    public Stream<SQLColumn> getSQLColumns(Class<?> cls) {
        return getColumnFields(cls)
                .map(field -> {
                    Column c = field.getAnnotation(Column.class);

                    return new SQLColumn(
                            c.name().isEmpty() ? field.getName().toLowerCase() : c.name(),
                            c.type().isEmpty() ? guessSQLColumnType(field.getType()) : c.type(),
                            c.additionals()
                    );
                });
    }

    public Stream<Field> getColumnFields(Class<?> cls) {
        return getDeclaredFields(cls).stream().filter(field -> field.isAnnotationPresent(Column.class)).peek(field -> field.setAccessible(true));
    }

    private List<Field> getDeclaredFields(Class<?> clazz) {
        return getDeclaredFields(getSuperTypes(clazz).toArray(new Class[0]));
    }

    private List<Field> getDeclaredFields(Class<?>... classes) {
        return flat(Arrays.stream(classes).map(aClass -> Arrays.stream(aClass.getDeclaredFields()).collect(Collectors.toList())).collect(Collectors.toList()));
    }

    private List<Class<?>> getSuperTypes(Class<?> baseClass) {
        List<Class<?>> supers = new ArrayList<>(Collections.singletonList(baseClass));

        while (baseClass.getSuperclass() != null) {
            supers.add(baseClass.getSuperclass());
            baseClass = baseClass.getSuperclass();
        }

        return supers;
    }

    private <T> List<T> flat(List<List<T>> list) {
        return list.stream().reduce((l1, l2) -> {
            l1.addAll(l2);
            return l1;
        }).orElse(new LinkedList<>());
    }
}