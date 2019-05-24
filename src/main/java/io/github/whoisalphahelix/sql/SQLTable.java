package io.github.whoisalphahelix.sql;

import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.Getter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Getter
public class SQLTable {
	
	private static final String INSERT = "INSERT INTO %s (%s) VALUES (%s);";
	private static final String SELECT_ALL = "SELECT * FROM %s";
	private static final String SELECT = SELECT_ALL + " WHERE %s = ?";
	private static final String SELECT_COLUMN = "SELECT %s FROM %s";
	private static final String REMOVE = "DELETE FROM %s WHERE(%S = ?)";
	private static final String DELETE = "DELETE FROm %s";
	private static final String UPDATE = "UPDATE %s SET %s = ? WHERE %s=?";
	
	private final SQL sql;
	private final String tableName;
	private final String tableInfo;
	private final SQLColumn[] SQLColumns;
	private final Map<String, Object> keyValueStore = new HashMap<>();
	
	public SQLTable(SQL sql, String tableName, SQLColumn[] SQLColumns) {
		this.sql = sql;
		this.SQLColumns = SQLColumns;
		
		StringBuilder tableInfoBuilder = new StringBuilder();
		
		for(SQLColumn c : SQLColumns)
			tableInfoBuilder.append(",").append(c.getName());
		
		this.tableName = tableName;
		this.tableInfo = tableInfoBuilder.replace(0, 1, "").toString();
	}
	
	public void insert(String... values) {
		StringBuilder sqlValues = new StringBuilder();
		for(String str : values)
			sqlValues.append(",").append("'").append(str).append("'");
		
		if(getSql().getDataSource().isRunning()) {
			String query = String.format(INSERT, tableName, getTableInfo(), sqlValues.toString().replaceFirst(",", ""));
			
			try {
				getSql().getDataSource().getConnection().prepareStatement(query).executeUpdate();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void insert(Object o) {
		if(!o.getClass().isAnnotationPresent(Table.class)) return;
		
		String[] values = this.sql.getHelix().reflections().getDeclaredFields(o.getClass(), true)
				.stream().filter(saveField -> saveField.isAnnotationPresent(io.github.whoisalphahelix.sql.annotations.Column.class))
				.map(saveField -> {
					if(saveField.asNormal().getType().isPrimitive())
						return saveField.get(o).toString();
					else
						return set(saveField.get(o));
				}).filter(s -> !s.isEmpty()).toArray(String[]::new);
		
		insert(values);
	}
	
	public List<List<String>> getAll() {
		if(!getSql().getDataSource().isRunning())
			return new LinkedList<>();
		
		List<List<String>> objs = new LinkedList<>();
		String query = String.format(SELECT_ALL, tableName);
		
		try {
			ResultSet rs = getSql().getDataSource().getConnection().prepareStatement(query).executeQuery();
			
			while(rs.next()) {
				List<String> rowObjects = new LinkedList<>();
				
				for(SQLColumn c : this.getSQLColumns())
					rowObjects.add(get(rs.getString(c.getName())));
				
				objs.add(rowObjects);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		return objs;
	}
	
	/*public List<List<String>> getSyncRows() {
		String qry = "SELECT * FROM " + this.getTableName() + ";";
		try {
			ResultSet rs = this.getConnector().connect(this.getTableClass()).prepareStatement(qry).executeQuery();
			LinkedList<List<String>> res = new LinkedList<List<String>>();
			while(rs.next()) {
				ArrayList<String> rowObjects = new ArrayList<String>();
				for(Column column : this.getColumns()) {
					rowObjects.add(rs.getString(column.name()));
				}
				res.add(rowObjects);
			}
			return res;
		} catch(NoConnectionException | SQLException e) {
			return new ArrayList<List<String>>();
		}
	}*/
	
	public <T> List<T> getAll(String column) {
		List<T> objs = new LinkedList<>();
		String query = String.format(SELECT_COLUMN, column, tableName);
		
		try {
			ResultSet rs = getSql().getDataSource().getConnection().prepareStatement(query).executeQuery();
			
			while(rs.next()) {
				objs.add(get(rs.getString(column)));
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		return objs;
	}
	
	public <T> T get(String column, Object key) {
		if(getSql().getDataSource().isRunning()) {
			String query = String.format(SELECT, tableName, column);
			
			try {
				PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
				prepstate.setString(1, this.sql.getHelix().utilHandler().getJsonUtil().toJsonTree(
						this.sql.getHelix().ioHandler().getGson(), key).toString());
				
				ResultSet rs = prepstate.executeQuery();
				
				if(rs == null)
					return null;
				
				if(rs.next()) {
					return get(rs.getString(column));
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	private <T> T get(String rs) {
		return (T) this.sql.getHelix().utilHandler().getJsonUtil().fromJsonTree(this.sql.getHelix().ioHandler().getGson(), rs);
	}
	
	private String set(Object o) {
		return this.sql.getHelix().utilHandler().getJsonUtil().toJsonTree(this.sql.getHelix().ioHandler().getGson(), o).toString();
	}
	
	public void remove(String column, Object key) {
		if(getSql().getDataSource().isRunning()) {
			String query = String.format(REMOVE, tableName, column);
			
			try {
				PreparedStatement prepstate = getSql().getDataSource().getConnection().prepareStatement(query);
				prepstate.setString(1, set(key));
				prepstate.executeUpdate();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void update(String checkColumn, Object key, String valueColumn, Object value) {
		if(getSql().getDataSource().isRunning()) {
			String query = String.format(UPDATE, tableName, valueColumn, checkColumn);
			
			try {
				PreparedStatement prep = getSql().getDataSource().getConnection().prepareStatement(query);
				prep.setString(1, set(value));
				prep.setString(2, set(key));
				prep.executeUpdate();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void delete() {
		if(getSql().getDataSource().isRunning()) {
			String query = String.format(DELETE, tableName);
			
			try {
				getSql().getDataSource().getConnection().prepareStatement(query).executeUpdate();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
