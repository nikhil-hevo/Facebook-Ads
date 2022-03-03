import com.facebook.ads.sdk.*;
import javafx.util.Duration;
import org.joda.time.DateTime;


import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.facebook.ads.sdk.AdsInsights.EnumLevel.VALUE_AD;

public class Main {
    public static void main(String[] args) throws Exception {
        List<String> fields = Arrays.asList(
                "date_start",
                "date_stop",
                "account_id",
                "account_name",
                "account_currency",
                "ad_id",
                "ad_name",
                "adset_id",
                "adset_name",
                "buying_type",
                "campaign_id",
                "campaign_name",
                "attribution_setting",
                "optimization_goal",
                "objective",
                "place_page_name",
                "action_values",
                "actions",
                "conversion_rate_ranking",
                "conversion_values",
                "conversions",
                "converted_product_quantity",
                "converted_product_value",
                "mobile_app_purchase_roas",
                "purchase_roas",
                "website_purchase_roas",
                "qualifying_question_qualify_answer_rate",
                "canvas_avg_view_percent",
                "canvas_avg_view_time",
                "clicks",
                "cost_per_action_type",
                "cost_per_conversion",
                "cost_per_estimated_ad_recallers",
                "cost_per_inline_link_click",
                "cost_per_inline_post_engagement",
                "cost_per_outbound_click",
                "catalog_segment_value",
                "cost_per_thruplay",
                "cost_per_unique_action_type",
                "cost_per_unique_click",
                "cost_per_unique_inline_link_click",
                "cost_per_unique_outbound_click",
                "estimated_ad_recall_rate",
                "estimated_ad_recallers",
                "full_view_impressions",
                "full_view_reach",
                "inline_link_click_ctr",
                "inline_link_clicks",
                "inline_post_engagement",
                "instant_experience_clicks_to_open",
                "instant_experience_clicks_to_start",
                "unique_actions",
                "video_30_sec_watched_actions",
                "video_avg_time_watched_actions",
                "video_p100_watched_actions",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p95_watched_actions",
                "video_play_actions",
                "video_play_curve_actions",
                "video_thruplay_watched_actions",
                "website_ctr",
                "cpc",
                "cpm",
                "cpp",
                "ctr",
                "engagement_rate_ranking",
                "frequency",
                "impressions",
                "outbound_clicks",
                "outbound_clicks_ctr",
                "quality_ranking",
                "reach",
                "social_spend",
                "spend",
                "unique_clicks",
                "unique_ctr",
                "unique_inline_link_click_ctr",
                "unique_inline_link_clicks",
                "unique_link_clicks_ctr",
                "unique_outbound_clicks",
                "unique_outbound_clicks_ctr"
        );
        LocalDate date = LocalDate.parse("2022-01-01");
        for (int i = 0; i < 60; i++, date = date.plusDays(1)) {
            LocalDate endDate = date.plusDays(1);
            AdAccount adAccount = new AdAccount(args[0], new APIContext(args[1]));
            AdAccount.APIRequestGetInsightsAsync getInsightsAsync = adAccount.getInsightsAsync()
                    .setLevel(VALUE_AD) // Ad level by default
                    .setFields(fields)
                    .setActionReportTime(AdsInsights.EnumActionReportTime.VALUE_IMPRESSION) // selected in source
                    .setBreakdowns(Collections.singletonList(AdsInsights.EnumBreakdowns.VALUE_PRODUCT_ID)) // Empty when not required
                    .setActionAttributionWindows(Arrays.asList(AdsInsights.EnumActionAttributionWindows.VALUE_7D_CLICK, AdsInsights.EnumActionAttributionWindows.VALUE_1D_VIEW)) // set attribution window
                    .setTimeIncrement("1") // to fetch the records on a granular level of per-day
                    .setTimeRange(getTimeRange(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
                    .setFiltering(getFiltering(args[2]));

            AdReportRun adReportRun = getInsightsAsync.execute();
            if (Objects.isNull(adReportRun)) {
                throw new Exception();
            }
            while (isJobRunning(adReportRun)) {
                Thread.sleep(1000);
            }
            System.out.println(adReportRun.getInsights().execute().getRawResponseAsJsonObject());
        }

    }

    public enum AsyncJobStatus {
        NOT_STARTED("Job Not Started"),
        STARTED("Job Started"),
        RUNNING("Job Running"),
        COMPLETED("Job Completed"),
        FAILED("Job Failed"),
        SKIPPED("Job Skipped");

        private String status;

        AsyncJobStatus(String status) {
            this.status = status;
        }

        public static AsyncJobStatus fromString(String status) throws IllegalArgumentException {
            for (AsyncJobStatus asyncJobStatus : AsyncJobStatus.values()) {
                if (asyncJobStatus.status.equalsIgnoreCase(status)) {
                    return asyncJobStatus;
                }
            }

            throw new IllegalArgumentException(String.format("%s is not a valid enum of type AsyncJobStatus", status));
        }
    }

    protected static boolean isJobRunning(AdReportRun adReportRun) throws Exception {
        AsyncJobStatus jobStatus = null;
        jobStatus = AsyncJobStatus.fromString(adReportRun.fetch().getFieldAsyncStatus());
        switch (jobStatus) {
            case NOT_STARTED:
            case STARTED:
            case RUNNING:
                return true;
            case COMPLETED:
                return adReportRun.getFieldAsyncPercentCompletion() == 100 ? false : true;
            default:
                throw new Exception();
        }
    }
    public static String getTimeRange(String startDate, String endDate) {
        return String.format("{\"since\":\"%s\",\"until\":\"%s\"}", startDate, endDate);
    }

    public static String getFiltering(String campaignId) {
        return String.format("[{\"field\":\"campaign.id\",\"operator\":\"EQUAL\", \"value\": \"%s\"}]", campaignId);
    }
}