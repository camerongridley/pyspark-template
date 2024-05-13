from pyspark_demo.utils import spark_session, math_ops
from pyspark_demo.utils.generate_data import generate_fake_user_data, generate_fake_activity, generate_fake_content_data
from pyspark_demo.utils.transformations import filter_active_users

# simple example of using an imported user created function
print('1 + 2 =', math_ops.add(1, 2))

spark = spark_session.get_spark_session()

n_users = 10
n_content = 20
n_activity = 30

fake_users = [u.json() for u in generate_fake_user_data(10)]
fake_content = [c.json() for c in generate_fake_content_data(10)]
fake_activity = [a.json() for a in generate_fake_activity(n_activity, n_users, n_content)]

# for user in fake_users:
#     print(user.model_dump_json(indent=2))
#
# for content in fake_content:
#     print(content.model_dump_json(indent=2))
#
# for activity in fake_activity:
#     print(activity.model_dump_json(indent=2))


users_rdd = spark.sparkContext.parallelize(fake_users)
users_raw_df = spark.read.json(users_rdd)
users_df = filter_active_users(users_raw_df)

content_rdd = spark.sparkContext.parallelize(fake_content)
content_df = spark.read.json(content_rdd)

activity_rdd = spark.sparkContext.parallelize(fake_activity)
activity_df = spark.read.json(activity_rdd)

joined_df = (users_df
             .join(activity_df, users_df.user_id == activity_df.user_id, 'inner')
             .join(content_df, content_df.content_id == activity_df.content_id, 'inner')
             .select(['username', 'is_active', 'title', 'latest_date_watched', 'latest_running_time'])
             )
joined_df.show()

#
# # Define schema for the DataFrame
# schema = StructType([
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True),
#     StructField("city", StringType(), True)
# ])
#
# # Data to be included in the DataFrame
# data = [
#     ("John Doe", 30, "New York"),
#     ("Jane Doe", 25, "Los Angeles"),
#     ("Mike Johnson", 35, "Chicago")
# ]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema)
#
# df.show()
#
# # Showing different syntax for filtering
# df2 = df.filter(df.age >= 30)
# df2.show()
# df3 = df.filter('age == 35')
# df3.show()
# df4 = df.filter(col('age') == 30)
# df4.show()
#
# data = [("James", "Sales", 3000),
#         ("Michael", "Sales", 4600),
#         ("Robert", "Sales", 4100),
#         ("Maria", "Finance", 3000),
#         ("James", "Sales", 3000),
#         ("Scott", "Finance", 3300),
#         ("Jen", "Finance", 3900),
#         ("Jeff", "Marketing", 3000),
#         ("Kumar", "Marketing", 2000),
#         ("Saif", "Sales", 4100)]
# columns = ["Employee_Name", "Department", "Salary"]
#
# df = spark.createDataFrame(data, columns)
# df_filtered = df.filter(col("Salary") > 3000)
# df_filtered.show()
#
