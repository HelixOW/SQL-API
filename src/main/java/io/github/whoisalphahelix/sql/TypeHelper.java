package io.github.whoisalphahelix.sql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TypeHelper {

    private static final Set<Class> WRAPPER_TYPES = new HashSet<>(Arrays.asList(
            Boolean.class, Character.class, Byte.class,
            Short.class, Integer.class, Long.class,
            Float.class, Double.class, Void.class, String.class));

    public static boolean isPrimitive(Class<?> clazz) {
        return clazz.isPrimitive() || WRAPPER_TYPES.contains(clazz);
    }
}
