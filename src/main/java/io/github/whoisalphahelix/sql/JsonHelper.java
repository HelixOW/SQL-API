package io.github.whoisalphahelix.sql;

import com.google.gson.*;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class JsonHelper {

    private static final JsonParser PARSER = new JsonParser();
    private static final Gson GSON = new GsonBuilder().create();

    public static Gson gson() {
        return GSON;
    }

    public String toMappedJson(Gson gson, String path, Object obj, Consumer<JsonObject> lastAdd) {
        List<JsonObject> jsonObjects = new LinkedList<>();
        String[] sections = path.split("\\.");

        for (int i = 0; i < sections.length; i++)
            jsonObjects.add(i, new JsonObject());

        for (int i = 0; i < sections.length; i++) {
            JsonObject current = jsonObjects.get(i);
            JsonObject top = getBefore(jsonObjects, i);

            if (i != 0)
                top.add(sections[i - 1], current);

            if (i == sections.length - 1) {
                current.add(sections[i], gson.toJsonTree(obj));
                lastAdd.accept(current);
            }
        }

        return jsonObjects.get(0).toString();
    }

    private JsonObject getBefore(List<JsonObject> objects, int id) {
        return objects.get(id == 0 ? 0 : id - 1);
    }

    public JsonElement toJsonTree(Gson gson, Object obj) {
        JsonObject head = new JsonObject();
        if (obj instanceof Number || obj instanceof String || obj instanceof Boolean || obj instanceof Character) {
            return gson.toJsonTree(obj);
        }
        head.add("body", gson.toJsonTree(obj));
        head.addProperty("type", obj.getClass().getName());
        return head;
    }

    public Object fromJsonTree(Gson gson, String json) {
        if (!json.contains("body") || !json.contains("type")) {
            JsonPrimitive primitive = (JsonPrimitive) PARSER.parse(json);
            if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            }
            if (primitive.isNumber()) {
                return findNumberType(primitive.getAsNumber());
            }
            if (primitive.isString()) {
                return primitive.getAsString();
            }
        }
        try {
            JsonObject obj = (JsonObject) PARSER.parse(json);
            return gson.fromJson(obj.get("body"), Class.forName(obj.get("type").getAsString()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Object findNumberType(Number num) {
        String a = num.toString();
        try {
            return Integer.parseInt(a);
        } catch (NumberFormatException numberFormatException) {
            try {
                return Double.parseDouble(a);
            } catch (NumberFormatException numberFormatException2) {
                try {
                    return Float.parseFloat(a);
                } catch (NumberFormatException numberFormatException3) {
                    try {
                        return Long.parseLong(a);
                    } catch (NumberFormatException numberFormatException4) {
                        try {
                            return Byte.parseByte(a);
                        } catch (NumberFormatException numberFormatException5) {
                            try {
                                return Short.parseShort(a);
                            } catch (NumberFormatException numberFormatException6) {
                                return null;
                            }
                        }
                    }
                }
            }
        }
    }
}
