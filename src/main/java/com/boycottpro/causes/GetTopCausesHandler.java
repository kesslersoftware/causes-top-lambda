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
        try {
            String sub = JwtUtility.getSubFromRestEvent(event);
            if (sub == null) return response(401, "Unauthorized");
            Map<String, String> pathParams = event.getPathParameters();
            int limit =  Integer.parseInt(pathParams.get("limit"));
            if (limit < 1 ) {
                return response(400,"error : Missing limit in path");
            }
            List<CausesSubset> causes = getTopCauses(limit);
            String responseBody = objectMapper.writeValueAsString(causes);
            return response(200,responseBody);
        } catch (Exception e) {
            return response(500,"error : Unexpected server error: " + e.getMessage());
        }
    }
    private APIGatewayProxyResponseEvent response(int status, String body) {
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(body);
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