-- Notification
-- Slack 
-- IMPORTANT::create slack webhook secret 
-- The secret will be generic string secret is the path segment form the slack webhook url 
-- e.g. https://hooks.slack.com/services/<slack webhook secret content>

desc secret kamesh_demos.alerts_and_notification.slack_demomate_webhook;

-- create the notification integration

-- Send message to App
create or replace notification integration kameshs_slack_demomate
  type = WEBHOOK
  enabled = true
  webhook_url = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  webhook_secret = kamesh_demos.alerts_and_notification.slack_demomate_webhook
  webhook_body_template='SNOWFLAKE_WEBHOOK_MESSAGE'
  webhook_headers=('Content-Type'='application/json');

-- send to channel 
create or replace notification integration kameshs_slack_alerts_notifications
  type = WEBHOOK
  enabled = true
  webhook_url = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  webhook_secret = kamesh_demos.alerts_and_notification.slack_alerts_notifications_webhook
  webhook_body_template='SNOWFLAKE_WEBHOOK_MESSAGE'
  webhook_headers=('Content-Type'='application/json');
