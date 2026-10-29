#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif

#include <iostream>
#include <memory>

int main() {
  const std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> conf{
      rd_kafka_conf_new(), rd_kafka_conf_destroy};
  const auto require_option = [&](const char *name, const char *value) {
    char error[512]{};
    if (rd_kafka_conf_set(conf.get(), name, value, error, sizeof(error)) !=
        RD_KAFKA_CONF_OK) {
      std::cerr << name << ": " << error << '\n';
      return false;
    }
    return true;
  };
  // Setting builtin.features fails when the linked library lacks SSL, even
  // though other TLS properties can still exist in a plaintext-only build.
  if (!require_option("builtin.features", "ssl") ||
      !require_option("security.protocol", "SSL") ||
      !require_option("enable.ssl.certificate.verification", "true") ||
      !require_option("ssl.endpoint.identification.algorithm", "https") ||
      !require_option("security.protocol", "SASL_SSL")) {
    return 1;
  }
  return 0;
}
