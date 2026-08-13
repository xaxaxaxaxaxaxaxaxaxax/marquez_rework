/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api.exceptions;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

class JsonProcessingExceptionMapperTest {
  private static final String SENSITIVE_DETAIL = "password=do-not-return";
  private final JsonProcessingExceptionMapper underTest = new JsonProcessingExceptionMapper(false);

  @Test
  void mapsJsonParseExceptionToGenericBadRequest() {
    JsonParseException malformedJson = new JsonParseException((JsonParser) null, SENSITIVE_DETAIL);

    Response response = underTest.toResponse(malformedJson);

    assertEquals(400, response.getStatus());
    assertEquals(APPLICATION_JSON_TYPE, response.getMediaType());
    assertGenericResponse(response, 400, "Unable to process JSON");
  }

  @Test
  void mapsJsonGenerationExceptionToGenericInternalServerError() {
    JsonGenerationException generationFailure = new JsonGenerationException(SENSITIVE_DETAIL);

    Response response = underTest.toResponse(generationFailure);

    assertEquals(500, response.getStatus());
    assertEquals(APPLICATION_JSON_TYPE, response.getMediaType());
    assertGenericResponse(response, 500, "There was an error processing your request.");
  }

  @Test
  void mapsInvalidDefinitionExceptionToGenericInternalServerError() {
    InvalidDefinitionException invalidDefinition =
        InvalidDefinitionException.from(
            (JsonParser) null,
            SENSITIVE_DETAIL,
            TypeFactory.defaultInstance().constructType(Object.class));

    Response response = underTest.toResponse(invalidDefinition);

    assertEquals(500, response.getStatus());
    assertEquals(APPLICATION_JSON_TYPE, response.getMediaType());
    assertGenericResponse(response, 500, "There was an error processing your request.");
  }

  private void assertGenericResponse(Response response, int code, String expectedMessagePrefix) {
    ErrorMessage error = (ErrorMessage) response.getEntity();
    assertEquals(code, error.getCode().intValue());
    assertTrue(error.getMessage().startsWith(expectedMessagePrefix));
    assertNull(error.getDetails());
    assertFalse(error.toString().contains(SENSITIVE_DETAIL));
  }
}
