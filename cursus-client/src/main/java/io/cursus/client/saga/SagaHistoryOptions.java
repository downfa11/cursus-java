package io.cursus.client.saga;

/** Required service identity placed on every history v1 event. */
public record SagaHistoryOptions(String environmentId, String serviceName) {
  public SagaHistoryOptions {
    if (environmentId == null || environmentId.isBlank() || serviceName == null || serviceName.isBlank()) {
      throw new IllegalArgumentException("environmentId and serviceName are required");
    }
  }
}
