/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api.exceptions;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.dropwizard.jersey.errors.ErrorMessage;
import javax.ws.rs.core.Response;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.Test;

class JdbiExceptionExceptionMapperTest {
  private static final String SENSITIVE_DETAIL =
      "password=do-not-return; SQL statement: select secret from credentials";

  @Test
  void mapsJdbiFailureToGenericInternalServerError() {
    UnableToExecuteStatementException databaseFailure =
        new UnableToExecuteStatementException(SENSITIVE_DETAIL);

    Response response = new JdbiExceptionExceptionMapper().toResponse(databaseFailure);

    assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals(APPLICATION_JSON_TYPE, response.getMediaType());
    ErrorMessage error = (ErrorMessage) response.getEntity();
    assertEquals(INTERNAL_SERVER_ERROR.getStatusCode(), error.getCode().intValue());
    assertEquals(INTERNAL_SERVER_ERROR.getReasonPhrase(), error.getMessage());
    assertNull(error.getDetails());
    assertFalse(error.toString().contains(SENSITIVE_DETAIL));
  }
}
