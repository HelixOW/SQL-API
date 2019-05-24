import io.github.whoisalphahelix.helix.Helix;
import io.github.whoisalphahelix.helix.IHelix;
import io.github.whoisalphahelix.helix.handlers.IOHandler;
import io.github.whoisalphahelix.helix.hon.Hon;
import io.github.whoisalphahelix.sql.SQL;
import io.github.whoisalphahelix.sql.SQLTable;

public class SQLTest {
	public static void main(String[] args) {
		IHelix helix = new Helix();
		
		Hon obj = new Hon(helix);
		
		obj.set("driver", "org.sqlite.JDBC");
		obj.set("jdbc-path", "jdbc:sqlite:" + IOHandler.HOME + "/test.db");
		
		SQL sql = new SQL(helix, obj);
		
		SQLTable heroTable = sql.createTable(Hero.class);
		
		heroTable.update("name", "Zen", "damage", 50);
	}
}
