package com.boycottpro.causes;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.Causes;
import com.boycottpro.models.CausesSubset;
import com.boycottpro.utilities.CausesUtility;
import com.boycottpro.utilities.JwtUtility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GetTopCausesHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String TABLE_NAME = "";
    private final DynamoDbClient dynamoDb;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GetTopCausesHandler() {
        this.dynamoDb = DynamoDbClient.create();
    }

    public GetTopCausesHandler(DynamoDbClient dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        String sub = null;
        try {
            sub = JwtUtility.getSubFromRestEvent(event);
            if (sub == null) return response(401, Map.of("message", "Unauthorized"));
            Map<String, String> pathParams = event.getPathParameters();
            int limit =  Integer.parseInt(pathParams.get("limit"));
            if (limit < 1 ) {
                return response(400,Map.of("error"," Missing limit in path"));
            }
            List<CausesSubset> causes = getTopCauses(limit);
            return response(200,causes);
        } catch (Exception e) {
            System.out.println(e.getMessage() + " for user " + sub);
            return response(500,Map.of("error", "Unexpected server error: " + e.getMessage()) );
        }
    }
    private APIGatewayProxyResponseEvent response(int status, Object body) {
        String responseBody = null;
        try {
            responseBody = objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(responseBody);
    }
    private List<CausesSubset> getTopCauses(int limit) {
        ScanRequest scan = ScanRequest.builder()
                .tableName("causes")
                .projectionExpression("cause_id, cause_desc, follower_count")
                .build();

        ScanResponse response = dynamoDb.scan(scan);

        AtomicInteger rankCounter = new AtomicInteger(1);

        return response.items().stream()
                .filter(item -> item.containsKey("follower_count"))
                .sorted(Comparator.comparingInt(
                        item -> -Integer.parseInt(item.get("follower_count").n())
                ))
                .limit(limit)
                .map(item -> {
                    CausesSubset subset = CausesUtility.mapToSubset(item);
                    subset.setRank(rankCounter.getAndIncrement());
                    return subset;
                })
                .collect(Collectors.toList());
    }

}