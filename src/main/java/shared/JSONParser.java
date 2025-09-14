package shared;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

interface JsonValue {
    String toJSON();
    String toPrettyJSON(int indent);
}

class JsonString implements JsonValue {
    public String value;
    public String toJSON(){
        return "\"" + escapeString(this.value) + "\"";
    }

    // according to the RFC https://www.rfc-editor.org/rfc/rfc8259#section-8.1,
    // the following need to be escaped
    // " quotation mark
    // \     reverse solidus
    // /     solidus
    // b     backspace
    // f     form feed
    // n     line feed
    // r     carriage return
    // t     tab
    // uXXXX unicode
    private String escapeString(String string) {
        StringBuilder sb = new StringBuilder();

        // for each char escape if needed
        for (char chr : string.toCharArray()) {
            switch (chr) {
                case '\"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '/':  sb.append("\\/");  break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    // check if character is in
                    // printable ASCII range
                    // 32 is smallest, a space
                    // 126 is the larges, a ~
                    if ((int) chr < 32 || (int) chr > 126) {
                        // non-ASCII
                        // formatting: "\\u%04x": u + 4 digit lower case hexadecimal
                        sb.append(String.format("\\u%04x", (int) chr));
                    } else {
                        // valid ascii char
                        sb.append(chr);
                    }
            }
        }
        return sb.toString();
    }

    public String toPrettyJSON(int indent){
        return toJSON();
    }
}

class JsonNumber implements JsonValue {
    public double value; // could be int or float
    public String toJSON(){
        return String.valueOf(value);
    }
    public String toPrettyJSON(int indent){
        return toJSON();
    }
}


class JsonBool implements JsonValue {
    public boolean value;
    public String toJSON(){
        return value ? "true" : "false";
    }

    public String toPrettyJSON(int indent){
        return toJSON();
    }
}

class JsonNull implements JsonValue {
    public String toJSON(){
        return "null";
    }

    public String toPrettyJSON(int indent){
        return toJSON();
    }
}


// nested Json
class JsonObject implements JsonValue {
    // using linked hashmap to preserve insertion order
    public LinkedHashMap<String, JsonValue> objects = new LinkedHashMap<>();

    public String toJSON(){

        StringBuilder sb = new StringBuilder();
        sb.append("{");

        int count = 0;

        for (Map.Entry<String, JsonValue> entry : objects.entrySet()) {
            if (count > 0) {
                sb.append(",");
            }
            count++;
            String field = entry.getKey();
            JsonValue value = entry.getValue();
            sb.append("\"").append(field).append("\":").append(value.toJSON());
        }
        sb.append("}");
        return sb.toString();
    }

    public String toPrettyJSON(int indent) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        String padding = " ".repeat(indent + 2);

        int count = 0;

        for (Map.Entry<String, JsonValue> entry : objects.entrySet()) {
            if (count > 0) {
                sb.append(",\n");
            }
            count++;
            String field = entry.getKey();
            JsonValue value = entry.getValue();
            sb.append(padding).append("\"").append(field).append("\": ").append(value.toPrettyJSON(indent + 2));
        }
        sb.append("\n").append(" ".repeat(indent)).append("}");
        return sb.toString();
    }
}

class JsonArray implements JsonValue {
    public ArrayList<JsonValue> elements = new ArrayList<>();

    public String toJSON(){
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        int count = 0;

        for (JsonValue element : elements) {
            if (count > 0) {
                sb.append(",");
            }
            count++;
            sb.append(element.toJSON());
        }
        sb.append("]");
        return sb.toString();
    }

    public String toPrettyJSON(int indent){
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        String padding = " ".repeat(indent + 2);

        int count = 0;

        for (JsonValue element : elements) {
            if (count > 0) {
                sb.append(",\n");
            }
            count++;
            sb.append(padding).append(element.toPrettyJSON(indent + 2));
        }
        sb.append("\n").append(" ".repeat(indent)).append("]");
        return sb.toString();
    }
}

public class JSONParser {

}
