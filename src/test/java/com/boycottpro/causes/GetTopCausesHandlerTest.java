package com.boycottpro.causes;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.boycottpro.causes.GetTopCausesHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GetTopCausesHandlerTest {

    @Mock
    private DynamoDbClient dynamoDb;

    @Mock
    private Context context;

    @InjectMocks
    private GetTopCausesHandler handler;

    @Test
    public void testHandleRequestReturnsTopCauses() throws Exception {
        int limit = 2;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);

        Map<String, AttributeValue> cause1 = Map.of(
                "cause_id", AttributeValue.fromS("cause1"),
                "cause_desc", AttributeValue.fromS("Environment"),
                "follower_count", AttributeValue.fromN("40")
        );
        Map<String, AttributeValue> cause2 = Map.of(
                "cause_id", AttributeValue.fromS("cause2"),
                "cause_desc", AttributeValue.fromS("Labor"),
                "follower_count", AttributeValue.fromN("30")
        );

        when(dynamoDb.scan(argThat((ScanRequest r) ->
                r != null && "causes".equals(r.tableName())
        ))).thenReturn(ScanResponse.builder().items(List.of(cause1, cause2)).build());


        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        assertEquals(200, response.getStatusCode());
        assertTrue(response.getBody().contains("Environment"));
        assertTrue(response.getBody().contains("Labor"));
        assertTrue(response.getBody().contains("40"));
        assertTrue(response.getBody().contains("30"));
    }

    @Test
    public void testHandleRequestReturns400ForMissingLimit() {
        int limit = 0;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        assertEquals(400, response.getStatusCode());
        assertTrue(response.getBody().contains("Missing limit"));
    }

    @Test
    public void testHandleRequestReturns500OnException() {
        int limit = 5;
        Map<String, String> pathParams = Map.of("limit", String.valueOf(limit));
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Path param "s" since client calls /users/s
        event.setPathParameters(pathParams);

        when(dynamoDb.scan(any(ScanRequest.class)))
                .thenThrow(RuntimeException.class);

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, context);

        assertEquals(500, response.getStatusCode());
        assertTrue(response.getBody().contains("Unexpected server error"));
    }
}