package io.github.whoisalphahelix.sql;

import com.google.gson.*;

public class JsonHelper {

    private static final JsonParser PARSER = new JsonParser();
    private static final Gson GSON = new GsonBuilder().create();

    public static Gson gson() {
        return GSON;
    }

    public JsonElement toJsonTree(Gson gson, Object obj) {
        JsonObject head = new JsonObject();

        if (TypeHelper.isPrimitive(obj.getClass()))
            return gson.toJsonTree(obj);

        head.add("body", gson.toJsonTree(obj));
        head.addProperty("type", obj.getClass().getName());
        return head;
    }

    public String toJsonTreeString(Gson gson, Object obj) {
        JsonObject head = new JsonObject();

        if (TypeHelper.isPrimitive(obj.getClass()))
            return gson.toJsonTree(obj.toString()).toString();

        head.add("body", gson.toJsonTree(obj));
        head.addProperty("type", obj.getClass().getName());
        return "\"" + head + "\"";
    }

    public Object fromJsonTree(Gson gson, String json) {
        if (!json.contains("body") || !json.contains("type")) {
            JsonPrimitive primitive = (JsonPrimitive) PARSER.parse(unescape(json));
            if (primitive.isBoolean())
                return primitive.getAsBoolean();
            else if (primitive.isNumber())
                return findNumberType(primitive.getAsNumber());
            else if (primitive.isString())
                return primitive.getAsString();
        }
        try {
            JsonObject obj = (JsonObject) PARSER.parse(unescape(json));
            return gson.fromJson(obj.get("body"), Class.forName(obj.get("type").getAsString()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String unescape(String json) {
        return json.replace("\\\"", "\"");
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
