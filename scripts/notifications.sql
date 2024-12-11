-- Notification
-- Slack 
-- IMPORTANT::create slack webhook secret 
-- The secret will be generic string secret is the path segment form the slack webhook url 
-- e.g. https://hooks.slack.com/services/<slack webhook secret content>

desc secret kamesh_demos.alerts_and_notification.slack_demomate_webhook;

-- create the notification integration

-- Send message to App
CREATE OR REPLACE NOTIFICATION INTEGRATION kameshs_slack_demomate
  TYPE = WEBHOOK
  ENABLED = true
  WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET = kamesh_demos.alerts_and_notification.slack_demomate_webhook
  WEBHOOK_BODY_TEMPLATE='SNOWFLAKE_WEBHOOK_MESSAGE'
  WEBHOOK_HEADERS=('Content-Type'='application/json');

-- send to channel 
CREATE OR REPLACE NOTIFICATION INTEGRATION kameshs_slack_alerts_notifications
  TYPE = WEBHOOK
  ENABLED = true
  WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET = alerts_and_notification.slack_alerts_notifications_webhook
  WEBHOOK_BODY_TEMPLATE='SNOWFLAKE_WEBHOOK_MESSAGE'
  WEBHOOK_HEADERS=('Content-Type'='application/json');

-- view integrations
show NOTIFICATION integrations;