package io.github.whoisalphahelix.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.whoisalphahelix.helix.IHelix;
import io.github.whoisalphahelix.helix.hon.Hon;
import io.github.whoisalphahelix.helix.reflection.SaveField;
import io.github.whoisalphahelix.sql.annotations.Column;
import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.sql.SQLException;
import java.util.Arrays;

@Getter
@EqualsAndHashCode
@ToString
public class SQL {
	
	private static final String CREATE = "CREATE TABLE IF NOT EXISTS %s (%s);";
	private static final String DROP = "DROP TABLE %s";
	
	private final HikariDataSource dataSource;
	private final IHelix helix;
	
	public SQL(IHelix helix, Hon object) {
		HikariConfig config = new HikariConfig();
		
		config.setDriverClassName(object.get("driver"));
		config.setJdbcUrl(object.get("jdbc-path"));
		
		if(object.contains("username"))
			config.setUsername(object.get("username"));
		if(object.contains("password"))
			config.setPassword(object.get("password"));
		
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		
		this.dataSource = new HikariDataSource(config);
		this.helix = helix;
	}
	
	public HikariDataSource getDataSource() {
		return dataSource;
	}
	
	public SQLTable createTable(String table, SQLColumn... sqlColumns) {
		StringBuilder infoBuilder = new StringBuilder();
		
		for(SQLColumn SQLColumn : sqlColumns)
			infoBuilder.append(SQLColumn.toSQL()).append(",");
		
		String info = infoBuilder.reverse().replace(0, 1, "").reverse().toString();
		String query = String.format(CREATE, table, info);
		
		try {
			System.out.println(query);
			getDataSource().getConnection().prepareStatement(query).execute();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		return new SQLTable(this, table, sqlColumns);
	}
	
	public SQLTable createTable(Class<?> table) {
		Table tab = table.getAnnotation(Table.class);
		
		if(tab == null) return null;
		
		String name = (tab.value().isEmpty() ? table.getSimpleName() : tab.value());
		
		return this.createTable(name, table);
	}
	
	public SQLTable createTable(String name, Class<?> table) {
		Column[] columns = this.helix.reflections().getDeclaredFields(table, true).stream()
				.filter(saveField -> saveField.isAnnotationPresent(Column.class))
				.map(saveField -> saveField.getAnnotation(Column.class)).toArray(Column[]::new);
		
		return createTable(name, Arrays.stream(columns).map(column -> new SQLColumn(
				column.name(), column.type(), column.additionals())).toArray(SQLColumn[]::new));
	}
	
	public void dropTable(String table) {
		try {
			getDataSource().getConnection().prepareStatement(String.format(DROP, table)).executeUpdate();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	private boolean isPrimitive(SaveField field) {
		return field.asNormal().getType().isPrimitive();
	}
}