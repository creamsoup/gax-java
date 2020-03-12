package com.google.api.gax.grpc;

import static com.google.common.base.Preconditions.checkState;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
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
    System.out.println("GENERATING RLS SERVICE CONFIG");
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
        + "          \"service\": \"google.bigtable.v2.Bigtable\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"headers\": [\n"
        + "        {\n"
        + "          \"key\": \"x-goog-request-params\","
        + "          \"names\": [\"x-goog-request-params\"]\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"lookupService\": \"cloud-bigtable-rls-test.main.gslb.googleprod.com\",\n"
        + "  \"lookupServiceTimeout\": 5,\n"
        + "  \"maxAge\": 120,\n"
        + "  \"validTargets\": ["
        + "    \"cloud-bigtable-api-test\", " // check if this is gslb
        + "    \"cloud-bigtable-api-test-us-central1\""
        + "  ],"
        + "  \"defaultTarget\": \"cloud-bigtable-api-test\",\n"
        + "  \"requestProcessingStrategy\": \"SYNC_LOOKUP_DEFAULT_TARGET_ON_ERROR\"\n"
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
