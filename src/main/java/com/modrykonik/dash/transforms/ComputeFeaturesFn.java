package com.modrykonik.dash.transforms;

import com.modrykonik.dash.model.UserStatsComputedRow;
import com.modrykonik.dash.model.UserStatsRow;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Compute features from data in BQ row
 */
public class ComputeFeaturesFn extends DoFn<UserStatsRow, UserStatsComputedRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        UserStatsRow data = c.element();

        UserStatsComputedRow ucrow = new UserStatsComputedRow();
        ucrow.day = data.day;
        ucrow.auth_user_id = data.auth_user_id;

        // Blogy - aktivita
        ucrow.is_photoblog_active =
            data.num_photoblog_posts > 0 ||
            data.num_photoblog_comments > 0 ||
            data.num_photoblog_likes_given > 0 ||
            data.num_photoblog_likes_given_post > 0;

        // Skupiny - aktivita
        ucrow.is_group_active =
            data.num_group_joined > 0 ||
            data.num_group_posts > 0 ||
            data.num_group_post_comments > 0 ||
            data.num_group_likes_given_post > 0 ||
            data.num_groups > 0;

        // Blogy a Skupiny - aktivita
        ucrow.is_pbandgroup_active =
            ucrow.is_photoblog_active ||
            ucrow.is_group_active;

        // Fórum - aktivita
        ucrow.is_forum_active =
            data.num_forum_threads > 0 ||
            data.num_forum_messages > 0 ||
            data.num_forum_likes_given_thread > 0 ||
            data.num_forum_likes_given_message > 0;

        // Bazár - aktivita
        ucrow.is_bazar_active =
            data.num_bazar_products > 0 ||
            data.num_bazar_products_reposted > 0 ||
            data.num_bazar_reviews > 0 ||
            data.num_bazar_transaction_message_to_seller > 0 ||
            data.num_bazar_transaction_message_to_buyer > 0 ||
            data.num_bazar_interest_made > 0 ||
            data.num_bazar_wishlist_added > 0 ||
            data.num_bazar_likes_given > 0;

        // Wiki - aktivita
        ucrow.is_wiki_active =
            data.num_wiki_experiences > 0 ||
            data.num_wiki_likes_given_experience > 0;

        // IP - aktivita
        ucrow.is_ip_active =
            data.num_ip_sent > 0 ||
            data.num_ip_starred > 0;

        // Srdiečka - aktivita
        ucrow.is_hearts_active =
            data.num_hearts_given > 0;

        //AARRR - aktivita na stránke
        ucrow.is_active =
            ucrow.is_photoblog_active ||
            ucrow.is_group_active ||
            ucrow.is_forum_active ||
            ucrow.is_bazar_active ||
            ucrow.is_wiki_active ||
            ucrow.is_ip_active ||
            ucrow.is_hearts_active;

        //AARRR - alive na stránke
        // kym sme nezbierali num_minutes_on_site, tak
        // za is_alive povazuj aj ked spravil login alebo aktivnu akciu
        ucrow.is_alive =
            data.num_logins > 0 ||
            data.num_minutes_on_site > 0 ||
            ucrow.is_active;

        ucrow.is_bazar_alive = data.num_minutes_on_site_forum > 0 || ucrow.is_bazar_active;
        ucrow.is_forum_alive = data.num_minutes_on_site_bazar > 0 || ucrow.is_forum_active;
        ucrow.is_group_alive = data.num_minutes_on_site_group > 0 || ucrow.is_group_active;
        ucrow.is_photoblog_alive = data.num_minutes_on_site_photoblog > 0 || ucrow.is_photoblog_active;

        ucrow.tmp_has_registered = data.has_registered;

        ucrow.is_desktop = data.num_minutes_on_site_desktop > 0;
        ucrow.is_mobile = data.num_minutes_on_site_mobile > 0;

        ucrow.num_words =
            data.num_words_bazaar +
            data.num_words_forum +
            data.num_words_photoblog_album +
            data.num_words_photoblog_article +
            data.num_words_photoblog_short_message +
            data.num_words_wiki_article +
            data.num_words_wiki_experience;

        c.output(ucrow);
    }
}
