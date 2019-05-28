package io.github.whoisalphahelix.sql;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class SQLKey<K> {
    private SQLColumn column;
    private K key;
}
