import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
  GetCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    const queryParams = event.queryStringParameters;
    if (!queryParams || !queryParams.movieId) {
      return {
        statusCode: 500,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing movie Id parameter" }),
      };
    }

    const movieId = parseInt(queryParams.movieId);
    let commandInput: QueryCommandInput = {
      TableName: process.env.CAST_TABLE_NAME,
      KeyConditionExpression: "movieId = :m",
      ExpressionAttributeValues: { ":m": movieId },
    };

    if ("roleName" in queryParams) {
      commandInput = {
        ...commandInput,
        IndexName: "roleIx",
        KeyConditionExpression: "movieId = :m and begins_with(roleName, :r)",
        ExpressionAttributeValues: { ":m": movieId, ":r": queryParams.roleName },
      };
    } else if ("actorName" in queryParams) {
      commandInput = {
        ...commandInput,
        KeyConditionExpression: "movieId = :m and begins_with(actorName, :a)",
        ExpressionAttributeValues: { ":m": movieId, ":a": queryParams.actorName },
      };
    }

    // 获取演员列表
    const castCommandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
    const castMembers = castCommandOutput.Items || [];

    // 处理 `facts=true`，获取电影详情
    let movieDetails = null;
    if (queryParams.facts === "true") {
      const movieCommandInput: GetCommandInput = {
        TableName: process.env.MOVIE_TABLE_NAME,
        Key: { movieId },
      };

      const movieCommandOutput = await ddbDocClient.send(new GetCommand(movieCommandInput));
      movieDetails = movieCommandOutput.Item || null;
    }

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        movie: movieDetails,
        cast: castMembers,
      }),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  return DynamoDBDocumentClient.from(ddbClient, {
    marshallOptions: { convertEmptyValues: true, removeUndefinedValues: true, convertClassInstanceToMap: true },
    unmarshallOptions: { wrapNumbers: false },
  });
}
