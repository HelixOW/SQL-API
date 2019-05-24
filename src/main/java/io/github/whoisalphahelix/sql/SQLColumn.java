package io.github.whoisalphahelix.sql;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@ToString
@EqualsAndHashCode
public class SQLColumn {
	
	private final String name, type;
	private final List<String> additional;
	
	public SQLColumn(String name, String type, String... additional) {
		this.name = name;
		this.type = type;
		this.additional = new ArrayList<>(Arrays.asList(additional));
	}
	
	public String toSQL() {
		if(additional.isEmpty())
			return name + " " + type;
		return name + " " + type + " " + additional.toString()
				.replace("[", "")
				.replace("]", "");
	}
}
