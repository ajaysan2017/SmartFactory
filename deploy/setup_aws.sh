#!/bin/bash
# Smart Factory - AWS backend setup
# ------------------------------------------
# Run this from AWS Cloud9 or an EC2 instance with the AWS CLI configured
# (AWS Academy Learner Lab credentials rotate each session - re-run `aws
# configure` or refresh credentials before running this if it's a new
# session). Uses the pre-provisioned LabRole for every service, exactly like
# Labs 1-4.
#
# Usage: bash setup_aws.sh
#
# This script is idempotent-ish (uses `|| true` on create calls) but it is
# meant to be read and run step by step, the same way the lab PDFs are -
# comment out sections you've already completed manually via the console.

set -e

REGION="us-east-1"                     # match your AWS Academy Learner Lab region
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
LABROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/LabRole"

TABLE_NAME="SmartFactory-SensorData"
BUCKET_NAME="smartfactory-datalake-${ACCOUNT_ID}"
STREAM_NAME="SmartFactory-datalake-stream"
SNS_TOPIC_NAME="SmartFactory-anomaly-alerts"
INGEST_FN="SmartFactory-ingest-processor"
API_FN="SmartFactory-api"
YOUR_EMAIL="REPLACE_WITH_YOUR_EMAIL@example.com"

echo "== Account: $ACCOUNT_ID | Region: $REGION =="

# ---------------------------------------------------------------------------
# 1. DynamoDB table + GSI  (Lab 3 pattern)
# ---------------------------------------------------------------------------
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
      AttributeName=device_id,AttributeType=S \
      AttributeName=sk,AttributeType=S \
      AttributeName=sensor_type,AttributeType=S \
      AttributeName=timestamp,AttributeType=S \
  --key-schema \
      AttributeName=device_id,KeyType=HASH \
      AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
      '[{"IndexName":"SensorTypeIndex","KeySchema":[{"AttributeName":"sensor_type","KeyType":"HASH"},{"AttributeName":"timestamp","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" || true

# ---------------------------------------------------------------------------
# 2. S3 bucket for the data lake (Firehose destination)
# ---------------------------------------------------------------------------
aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" || true

# ---------------------------------------------------------------------------
# 3. Kinesis Firehose delivery stream -> S3 (Lab 4, Experiment 3)
# ---------------------------------------------------------------------------
aws firehose create-delivery-stream \
  --delivery-stream-name "$STREAM_NAME" \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration \
      "RoleARN=${LABROLE_ARN},BucketARN=arn:aws:s3:::${BUCKET_NAME},Prefix=raw/,BufferingHints={SizeInMBs=1,IntervalInSeconds=60}" \
  --region "$REGION" || true

# ---------------------------------------------------------------------------
# 4. SNS topic for anomaly alerts (Lab 2 pattern) + email subscription
# ---------------------------------------------------------------------------
SNS_TOPIC_ARN=$(aws sns create-topic --name "$SNS_TOPIC_NAME" --region "$REGION" --query TopicArn --output text)
aws sns subscribe --topic-arn "$SNS_TOPIC_ARN" --protocol email --notification-endpoint "$YOUR_EMAIL" --region "$REGION"
echo "SNS topic: $SNS_TOPIC_ARN  (check your inbox and confirm the subscription)"

# ---------------------------------------------------------------------------
# 5. Package + deploy the ingest Lambda
# ---------------------------------------------------------------------------
cd "$(dirname "$0")/../backend"
zip -j /tmp/ingest.zip lambda_ingest.py

aws lambda create-function \
  --function-name "$INGEST_FN" \
  --runtime python3.12 \
  --role "$LABROLE_ARN" \
  --handler lambda_ingest.handler \
  --zip-file fileb:///tmp/ingest.zip \
  --timeout 30 \
  --environment "Variables={DYNAMODB_TABLE=${TABLE_NAME},FIREHOSE_STREAM=${STREAM_NAME},SNS_TOPIC_ARN=${SNS_TOPIC_ARN}}" \
  --region "$REGION" || \
aws lambda update-function-code --function-name "$INGEST_FN" --zip-file fileb:///tmp/ingest.zip --region "$REGION"

INGEST_FN_ARN=$(aws lambda get-function --function-name "$INGEST_FN" --region "$REGION" --query 'Configuration.FunctionArn' --output text)

# ---------------------------------------------------------------------------
# 6. Package + deploy the API Lambda
# ---------------------------------------------------------------------------
zip -j /tmp/api.zip lambda_api.py

aws lambda create-function \
  --function-name "$API_FN" \
  --runtime python3.12 \
  --role "$LABROLE_ARN" \
  --handler lambda_api.handler \
  --zip-file fileb:///tmp/api.zip \
  --timeout 15 \
  --environment "Variables={DYNAMODB_TABLE=${TABLE_NAME},SENSOR_TYPE_INDEX=SensorTypeIndex}" \
  --region "$REGION" || \
aws lambda update-function-code --function-name "$API_FN" --zip-file fileb:///tmp/api.zip --region "$REGION"

API_FN_ARN=$(aws lambda get-function --function-name "$API_FN" --region "$REGION" --query 'Configuration.FunctionArn' --output text)

# ---------------------------------------------------------------------------
# 7. IoT Core topic rule -> ingest Lambda  (Lab 2/4 pattern)
# ---------------------------------------------------------------------------
sed "s#REPLACE_WITH_ARN_OF_SmartFactory-ingest-processor#${INGEST_FN_ARN}#" iot_rule.json > /tmp/iot_rule.json

aws iot create-topic-rule \
  --rule-name SmartFactoryIngestRule \
  --topic-rule-payload file:///tmp/iot_rule.json \
  --region "$REGION" || true

aws lambda add-permission \
  --function-name "$INGEST_FN" \
  --statement-id "IoTRuleInvoke" \
  --action "lambda:InvokeFunction" \
  --principal iot.amazonaws.com \
  --source-arn "arn:aws:iot:${REGION}:${ACCOUNT_ID}:rule/SmartFactoryIngestRule" \
  --region "$REGION" || true

# ---------------------------------------------------------------------------
# 8. API Gateway (HTTP API) -> API Lambda, with the 3 routes the dashboard uses
# ---------------------------------------------------------------------------
API_ID=$(aws apigatewayv2 create-api \
  --name SmartFactory-api \
  --protocol-type HTTP \
  --target "$API_FN_ARN" \
  --region "$REGION" --query ApiId --output text)

# `--target` above already creates a $default route + integration; add the
# explicit routes/paths our dashboard calls so path parameters resolve.
INTEGRATION_ID=$(aws apigatewayv2 get-integrations --api-id "$API_ID" --region "$REGION" --query 'Items[0].IntegrationId' --output text)

aws apigatewayv2 create-route --api-id "$API_ID" --route-key "GET /readings" --target "integrations/${INTEGRATION_ID}" --region "$REGION" || true
aws apigatewayv2 create-route --api-id "$API_ID" --route-key "GET /stats" --target "integrations/${INTEGRATION_ID}" --region "$REGION" || true
aws apigatewayv2 create-route --api-id "$API_ID" --route-key "GET /device/{device_id}/history" --target "integrations/${INTEGRATION_ID}" --region "$REGION" || true

aws lambda add-permission \
  --function-name "$API_FN" \
  --statement-id "ApiGatewayInvoke" \
  --action "lambda:InvokeFunction" \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${API_ID}/*/*" \
  --region "$REGION" || true

API_ENDPOINT="https://${API_ID}.execute-api.${REGION}.amazonaws.com"
echo ""
echo "======================================================================"
echo "Done. Dashboard API base URL:  $API_ENDPOINT"
echo "Paste this into dashboard/index.html's 'Connect' field."
echo "======================================================================"
