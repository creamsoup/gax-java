package com.google.api.gax.grpc;

import static com.google.common.base.Preconditions.checkState;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.grpc.internal.JsonParser;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RlsProvider {

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getRlsServiceConfig() {
    String rlsConfigJson = getRlsConfigJsonStr();
    String grpclbJson = "{\"grpclb\": {\"childPolicy\": [{\"pick_first\": {}}]}}";
    String serviceConfig = "{"
        + "\"loadBalancingConfig\": [{"
        + "    \"rls\": {"
        + "      \"routeLookupConfig\": " + rlsConfigJson + ", "
        + "      \"childPolicy\": [" + grpclbJson + "],"
        + "      \"childPolicyConfigTargetFieldName\": \"serviceName\""
        + "      }"
        + "  }]"
        + "}";
    try {
      Map<String, Object> foo =
          (Map<String, Object>) io.grpc.internal.JsonParser.parse(serviceConfig);
      System.out.println("ServiceConfig used: " + foo);
      return foo;
    } catch (IOException e) {
      throw new RuntimeException("generating rls config failed, this shouldn't happen", e);
    }
  }

  private static String getRlsConfigJsonStr() {
    return "{\n"
        + "  \"grpcKeyBuilders\": [\n"
        + "    {\n"
        + "      \"names\": [\n"
        + "        {\n"
        + "          \"service\": \"grpc.lookup.v1.BackendService\",\n"
        + "          \"method\": \"Echo\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"headers\": [\n"
        + "        {\n"
        + "          \"key\": \"user\","
        + "          \"names\": [\"User\", \"Parent\"],\n"
        + "          \"optional\": true\n"
        + "        },\n"
        + "        {\n"
        + "          \"key\": \"id\","
        + "          \"names\": [\"X-Google-Id\"],\n"
        + "          \"optional\": true\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"names\": [\n"
        + "        {\n"
        + "          \"service\": \"grpc.lookup.v1.BackendService\",\n"
        + "          \"method\": \"*\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"headers\": [\n"
        + "        {\n"
        + "          \"key\": \"user\","
        + "          \"names\": [\"User\", \"Parent\"],\n"
        + "          \"optional\": true\n"
        + "        },\n"
        + "        {\n"
        + "          \"key\": \"password\","
        + "          \"names\": [\"Password\"],\n"
        + "          \"optional\": true\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"lookupService\": \"localhost:8972\",\n"
        + "  \"lookupServiceTimeout\": 2,\n"
        + "  \"maxAge\": 300,\n"
        + "  \"staleAge\": 240,\n"
        + "  \"validTargets\": [\"localhost:9001\", \"localhost:9002\"],"
        + "  \"cacheSizeBytes\": 1000,\n"
        + "  \"defaultTarget\": \"defaultTarget\",\n"
        + "  \"requestProcessingStrategy\": \"ASYNC_LOOKUP_DEFAULT_TARGET_ON_MISS\"\n"
        + "}";
  }

  /**
   * Parses JSON with as few preconceived notions as possible.
   */
  public static final class JsonParser {

    private static final Logger logger =
        Logger.getLogger(io.grpc.internal.JsonParser.class.getName());

    private JsonParser() {}

    /**
     * Parses a json string, returning either a {@code Map<String, ?>}, {@code List<?>},
     * {@code String}, {@code Double}, {@code Boolean}, or {@code null}.
     */
    public static Object parse(String raw) throws IOException {
      JsonReader jr = new JsonReader(new StringReader(raw));
      try {
        return parseRecursive(jr);
      } finally {
        try {
          jr.close();
        } catch (IOException e) {
          logger.log(Level.WARNING, "Failed to close", e);
        }
      }
    }

    private static Object parseRecursive(JsonReader jr) throws IOException {
      checkState(jr.hasNext(), "unexpected end of JSON");
      switch (jr.peek()) {
        case BEGIN_ARRAY:
          return parseJsonArray(jr);
        case BEGIN_OBJECT:
          return parseJsonObject(jr);
        case STRING:
          return jr.nextString();
        case NUMBER:
          return jr.nextDouble();
        case BOOLEAN:
          return jr.nextBoolean();
        case NULL:
          return parseJsonNull(jr);
        default:
          throw new IllegalStateException("Bad token: " + jr.getPath());
      }
    }

    private static Map<String, ?> parseJsonObject(JsonReader jr) throws IOException {
      jr.beginObject();
      Map<String, Object> obj = new LinkedHashMap<>();
      while (jr.hasNext()) {
        String name = jr.nextName();
        Object value = parseRecursive(jr);
        obj.put(name, value);
      }
      checkState(jr.peek() == JsonToken.END_OBJECT, "Bad token: " + jr.getPath());
      jr.endObject();
      return Collections.unmodifiableMap(obj);
    }

    private static List<?> parseJsonArray(JsonReader jr) throws IOException {
      jr.beginArray();
      List<Object> array = new ArrayList<>();
      while (jr.hasNext()) {
        Object value = parseRecursive(jr);
        array.add(value);
      }
      checkState(jr.peek() == JsonToken.END_ARRAY, "Bad token: " + jr.getPath());
      jr.endArray();
      return Collections.unmodifiableList(array);
    }

    private static Void parseJsonNull(JsonReader jr) throws IOException {
      jr.nextNull();
      return null;
    }
  }
}
