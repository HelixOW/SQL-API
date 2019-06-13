import io.github.whoisalphahelix.sql.SQL;
import io.github.whoisalphahelix.sql.SQLTable;
import io.github.whoisalphahelix.sql.annotations.Column;
import io.github.whoisalphahelix.sql.annotations.Table;
import lombok.*;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

public class QuickTest {
    public static void main(String[] args) throws IOException {
        QuickTest test = new QuickTest();

        test.createFile(new File(System.getProperty("user.home") + "/tests/sqltest.db"));

        SQL sql = new SQL("org.sqlite.JDBC",
                "jdbc:sqlite:" + System.getProperty("user.home") + "/tests/sqltest.db");

        SQLTable<PlayerUUID> ids = sql.createTable(PlayerUUID.class, objects ->
                new PlayerUUID(UUID.fromString(objects.get(0).toString()),
                        objects.get(1).toString(),
                        Long.parseLong(objects.get(2).toString())));

        ids.insert(new PlayerUUID(UUID.randomUUID(), "Max", LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond()));

        System.out.println(ids.getAll());
    }

    public File createFile(File file) throws IOException {
        if (!file.exists() && !file.isDirectory()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }

        if (!file.setExecutable(true)) {
            System.out.println("Unable to set File executable (" + file.getAbsolutePath() + ")");
        }

        if (!file.setWritable(true)) {
            System.out.println("Unable to set File writable (" + file.getAbsolutePath() + ")");
        }

        if (!file.setReadable(true)) {
            System.out.println("Unable to set File readable (" + file.getAbsolutePath() + ")");
        }

        return file;
    }

    @Getter
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor(access = AccessLevel.PROTECTED)
    @Table("player_ids")
    public static final class PlayerUUID {
        @Column
        private final UUID id;
        @Column
        private final String name;
        @Setter
        @Column
        private long timeStamp;

        public LocalDateTime timeStampAsDate() {
            return Instant.ofEpochMilli(timeStamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
    }
}
