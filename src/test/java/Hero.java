import io.github.whoisalphahelix.sql.annotations.Column;
import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.AllArgsConstructor;

@AllArgsConstructor
@Table("OverwatchTest")
public class Hero {
	
	@Column(name = "name", type = "TEXT", additionals = {"PRIMARY KEY"})
	private String name;
	@Column(name = "type", type = "TEXT")
	private String type;
	@Column(name = "health", type = "INTEGER")
	private int health;
	@Column(name = "damage", type = "REAL")
	private double damage;
	
}
