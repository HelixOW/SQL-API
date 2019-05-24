import io.github.whoisalphahelix.sql.annotations.Table;

@Table("subheros")
public class SubHero extends Hero {
	public SubHero(String name, String type, int health, double damage) {
		super(name, type, health, damage);
	}
}
