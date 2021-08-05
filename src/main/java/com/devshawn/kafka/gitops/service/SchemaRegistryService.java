package com.devshawn.kafka.gitops.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.SaslConfigs;
import com.devshawn.kafka.gitops.config.SchemaRegistryConfig;
import com.devshawn.kafka.gitops.domain.plan.SchemaPlan;
import com.devshawn.kafka.gitops.domain.state.SchemaDetails;
import com.devshawn.kafka.gitops.enums.SchemaType;
import com.devshawn.kafka.gitops.exception.SchemaRegistryExecutionException;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.JsonDiff;
import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

public class SchemaRegistryService {

    private final SchemaRegistryConfig config;

    public SchemaRegistryService(SchemaRegistryConfig config) {
        this.config = config;
    }

    public List<String> getAllSubjects() {
        final CachedSchemaRegistryClient cachedSchemaRegistryClient = createSchemaRegistryClient();
        try {
            return new ArrayList<>(cachedSchemaRegistryClient.getAllSubjects());
        } catch (IOException | RestClientException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to get all schema registry subjects", ex.getMessage());
        }
    }

    public void deleteSubject(String subject, boolean isPermanent) {
        final CachedSchemaRegistryClient cachedSchemaRegistryClient = createSchemaRegistryClient();
        try {
            // must always soft-delete
            cachedSchemaRegistryClient.deleteSubject(subject);
            if (isPermanent) {
                cachedSchemaRegistryClient.deleteSubject(subject, true);
            }
        } catch (IOException | RestClientException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to get delete subject from schema registry", ex.getMessage());
        }
    }

    public int register(SchemaPlan schemaPlan) {
        final CachedSchemaRegistryClient cachedSchemaRegistryClient = createSchemaRegistryClient();
        ParsedSchema parsedSchema;
        if(SchemaType.AVRO.toString().equalsIgnoreCase(schemaPlan.getSchemaDetails().get().getType())) {
            AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
            parsedSchema = avroSchemaProvider.parseSchema(
                loadSchemaFromDisk(schemaPlan.getSchemaDetails().get().getFile()), Collections.emptyList()).get();
        } else if (SchemaType.JSON.toString().equalsIgnoreCase(schemaPlan.getSchemaDetails().get().getType())) {
            JsonSchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
            parsedSchema = jsonSchemaProvider.parseSchema(
                loadSchemaFromDisk(schemaPlan.getSchemaDetails().get().getFile()), Collections.emptyList()).get();
        } else if (SchemaType.PROTOBUF.toString().equalsIgnoreCase(schemaPlan.getSchemaDetails().get().getType())) {
            ProtobufSchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
            parsedSchema = protobufSchemaProvider.parseSchema(loadSchemaFromDisk(
                schemaPlan.getSchemaDetails().get().getFile()), Collections.emptyList()).get();
        } else {
            throw new ValidationException("Unknown schema type: " + schemaPlan.getSchemaDetails().get().getType());
        }
        try {
            return cachedSchemaRegistryClient.register(schemaPlan.getName(), parsedSchema);
        } catch (IOException | RestClientException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to register subject with schema registry", ex.getMessage());
        }
    }

    public void validateSchema(SchemaDetails schemaDetails) {
        if (schemaDetails.getType().equalsIgnoreCase(SchemaType.AVRO.toString())) {
            AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
            validateSchema(schemaDetails, avroSchemaProvider);
        } else if (schemaDetails.getType().equalsIgnoreCase(SchemaType.JSON.toString())) {
            JsonSchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
            validateSchema(schemaDetails, jsonSchemaProvider);
        } else if (schemaDetails.getType().equalsIgnoreCase(SchemaType.PROTOBUF.toString())) {
            ProtobufSchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
            validateSchema(schemaDetails, protobufSchemaProvider);
        } else {
            throw new ValidationException("Unknown schema type: " + schemaDetails.getType());
        }
    }

    public void validateSchema(SchemaDetails schemaDetails, AbstractSchemaProvider schemaProvider) {
      if (schemaDetails.getReferences().isEmpty()) {
          Optional<ParsedSchema> parsedSchema = schemaProvider.parseSchema(loadSchemaFromDisk(schemaDetails.getFile()), Collections.emptyList());
          if (!parsedSchema.isPresent()) {
              throw new ValidationException(String.format("%s schema %s could not be parsed.", schemaProvider.schemaType(), schemaDetails.getFile()));
          }
      } else {
          List<SchemaReference> schemaReferences = new ArrayList<>();
          schemaDetails.getReferences().forEach(referenceDetails -> {
              SchemaReference schemaReference = new SchemaReference(referenceDetails.getName(), referenceDetails.getSubject(), referenceDetails.getVersion());
              schemaReferences.add(schemaReference);
          });
          // we need to pass a schema registry client as a config because the underlying code validates against the current state
          schemaProvider.configure(Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, createSchemaRegistryClient()));
          try {
              Optional<ParsedSchema> parsedSchema = schemaProvider.parseSchema(loadSchemaFromDisk(schemaDetails.getFile()), schemaReferences);
              if (!parsedSchema.isPresent()) {
                  throw new ValidationException(String.format("%s schema %s could not be parsed.", schemaProvider.schemaType(), schemaDetails.getFile()));
              }
          } catch (IllegalStateException ex) {
              throw new ValidationException(String.format("Reference validation error: %s", ex.getMessage()));
          } catch (RuntimeException ex) {
              throw new ValidationException(String.format("Error thrown when attempting to validate %s schema with reference: %s", schemaProvider.schemaType(), ex.getMessage()));
          }
      }
    }

    public SchemaMetadata getLatestSchemaMetadata(String subject) {
        final CachedSchemaRegistryClient cachedSchemaRegistryClient = createSchemaRegistryClient();
        try {
            return cachedSchemaRegistryClient.getLatestSchemaMetadata(subject);
        } catch (IOException | RestClientException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to get delete subject from schema registry", ex.getMessage());
        }
    }

    public String compareSchemasAndReturnDiff(String schemaStringOne, String schemaStringTwo) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode schemaOne = objectMapper.readTree(schemaStringOne);
            JsonNode schemaTwo = objectMapper.readTree(schemaStringTwo);
            JsonNode diff = JsonDiff.asJson(schemaOne, schemaTwo);
            if (diff.isEmpty()) {
                return null;
            }
            return diff.toString();
        } catch (JsonProcessingException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to compare the schemas", ex.getMessage());
        }
    }

    public String loadSchemaFromDisk(String fileName) {
        final String SCHEMA_DIRECTORY = config.getConfig().get("SCHEMA_DIRECTORY").toString();
        try {
            return new String(Files.readAllBytes(Paths.get(SCHEMA_DIRECTORY + "/" + fileName)), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new SchemaRegistryExecutionException("Error thrown when attempting to load a schema from schema directory", ex.getMessage());
        }
    }

    public CachedSchemaRegistryClient createSchemaRegistryClient() {
        RestService restService = new RestService(config.getConfig().get("SCHEMA_REGISTRY_URL").toString());
        SaslBasicAuthCredentialProvider saslBasicAuthCredentialProvider = new SaslBasicAuthCredentialProvider();
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, config.getConfig().get("SCHEMA_REGISTRY_SASL_CONFIG").toString());
        saslBasicAuthCredentialProvider.configure(clientConfig);
        restService.setBasicAuthCredentialProvider(saslBasicAuthCredentialProvider);
        return new CachedSchemaRegistryClient(restService, 10);
    }

}
