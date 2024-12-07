
-- Alert - alerts when there is stronger negative feedback
-- Truck Review Alert
CREATE OR REPLACE ALERT alerts_and_notification.truck_review_alert
  SCHEDULE = '1 minute'
  IF(
      EXISTS(
        WITH negative_reviews AS (
            SELECT 
                truck_id,
                review,
                sentiment_score,
                ROW_NUMBER() OVER (PARTITION BY truck_id ORDER BY sentiment_score ASC) as worst_review_rank
            FROM data.truck_review_sentiments
            WHERE sentiment_class = 'negative'
            AND sentiment_score < -0.8
        )
        SELECT 
            truck_id,
            review,
            sentiment_score
        FROM negative_reviews
        WHERE worst_review_rank = 1
        ORDER BY sentiment_score ASC
        LIMIT 3 -- top 3 only
      )
    )
  THEN
    BEGIN
        -- TODO add event
        LET rs RESULTSET := (
            WITH REVIEW_DATA AS (
                    SELECT truck_id, review
                    FROM TABLE(RESULT_SCAN(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID()))
                ),
                SUMMARIZED_CONTENT AS (
                SELECT 
                    SNOWFLAKE.CORTEX.COMPLETE(
                        'llama3.1-405b',
                        CONCAT(
                            'Summarize the review as bullets formatted for slack notification blocks with right and consistent emojis and always add truck id to the Review Alert header along with truck emoji and stay consistent with Header like <alert emoji> Review  <alert emoji> <truck emoji> <space> Truck ID - <truck id>:',
                            '<REVIEW>', 
                            REVIEW, 
                            '</REVIEW>',
                            'Quote the truck id.', 
                            TRUCK_ID,
                            '.Generate only Slack blocks and strictly ignore other text.'
                        )) AS SUMMARY
                FROM REVIEW_DATA
            ),
            FORMATTED_BLOCKS AS (
                SELECT SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(SUMMARY) AS CLEAN_BLOCKS
                FROM SUMMARIZED_CONTENT
            ),
            JSON_BLOCKS AS (
                SELECT SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(CONCAT('{"blocks":',CLEAN_BLOCKS,'}')) AS BLOCKS
                FROM FORMATTED_BLOCKS
            )
            -- slack message content blocks
            SELECT BLOCKS FROM JSON_BLOCKS
        );
    
        FOR record IN rs DO
            let slack_message varchar := record.BLOCKS;
            SYSTEM$LOG_INFO('SLACK MESSAGE:',OBJECT_CONSTRUCT('slack_message', slack_message));
            CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                :slack_message,
                SNOWFLAKE.NOTIFICATION.INTEGRATION('KAMESHS_SLACK_ALERTS_NOTIFICATIONS')
            );
        END FOR;
    END;

-- 

show alerts in database kamesh_demos;
