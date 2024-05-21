from pyspark_template.utils import spark_session
from pyspark_template.utils.generate_data import generate_and_save_streaming_service_data
from pyspark_template.transformations.filtering import filter_active_users
from pyspark_template.transformations.feature_engineering import create_age_group

# parser = argparse.ArgumentParser(description='Spark job args')
# parser.add_argument('s3_user', type=str, default='', help='s3 prefix for the user data')
# parser.add_argument('s3_content', type=str, default='', help='s3 prefix for the content data')
# parser.add_argument('s3_activity', type=str, default='', help='s3 prefix for the activity data')
#
# args = parser.parse_args()
# s3_user = args.s3_user
# s3_content = args.s3_content
# s3_activity = args.s3_activity

# users_raw_df = spark.read.parquet(s3_user)
# content_raw_df = spark.read.parquet(s3_content)
# activity_raw_df = spark.read.parquet(s3_activity)
spark = spark_session.get_spark_session()

n_users = 10
n_content = 20
n_activity = 30

users_raw_df, content_raw_df, activity_raw_df = (
    generate_and_save_streaming_service_data(
        spark, n_users, n_content, n_activity, "../data/streaming"
    )
)

active_users_df = filter_active_users(users_raw_df)
active_age_grps_df = create_age_group(active_users_df)

joined_df = (
    active_age_grps_df.join(
        activity_raw_df, active_age_grps_df.user_id == activity_raw_df.user_id, "inner"
    )
    .join(
        content_raw_df, content_raw_df.content_id == activity_raw_df.content_id, "inner"
    )
    .select(
        ["username", "age_group", "is_active", "title", "latest_date_watched", "latest_running_time"]
    )
)
joined_df.show()
