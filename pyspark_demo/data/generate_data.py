from pyspark_demo.models.user import User
from pyspark_demo.models.content import Content
from pyspark_demo.models.activity import Activity
from faker import Faker
from pyspark.sql import SparkSession


def generate_fake_user_data(num_users: int):
    fake = Faker()
    users = []
    for _ in range(num_users):
        user = User(
            user_id=fake.unique.random_int(min=1, max=num_users),
            username=fake.user_name(),
            is_active=fake.boolean(),
            address=fake.street_address(),
            city=fake.city(),
            state=fake.state(),
            area_code=fake.postcode(),
            country=fake.country(),
            age=fake.random_int(min=12, max=100) if fake.boolean(chance_of_getting_true=80) else None
        )
        users.append(user)
    return users


def generate_fake_content_data(n_content: int):
    fake = Faker()
    ratings = ['G', 'PG', 'PG-13', 'R']
    content_list = []

    for _ in range(n_content):
        content = Content(
            content_id=fake.unique.random_int(min=1, max=n_content),
            title=fake.sentence(nb_words=5).rstrip('.'),
            rating=fake.random_element(elements=ratings),
            running_time=fake.random_int(min=30, max=180)
        )
        content_list.append(content)

    return content_list


def generate_fake_activity(n_activity: int, n_users: int, n_content: int):
    fake = Faker()
    activity_list = []

    for _ in range(n_activity):
        activity = Activity(
            activity_id=fake.unique.random_int(min=1, max=n_activity),
            user_id=fake.random_int(min=1, max=n_users),
            content_id=fake.random_int(min=1, max=n_content),
            latest_date_watched=fake.date_between(),
            latest_running_time=fake.random_int(min=1, max=180)
        )

        activity_list.append(activity)

    return activity_list


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

spark = (SparkSession.builder
         .appName("FakerDataframes")
         .getOrCreate()
         )

users_rdd = spark.sparkContext.parallelize(fake_users)
users_df = spark.read.json(users_rdd)

content_rdd = spark.sparkContext.parallelize(fake_content)
content_df = spark.read.json(content_rdd)

activity_rdd = spark.sparkContext.parallelize(fake_activity)
activity_df = spark.read.json(activity_rdd)

joined_df = (users_df
             .join(activity_df, users_df.user_id == activity_df.user_id, 'inner')
             .join(content_df, content_df.content_id == activity_df.content_id, 'inner')
             .select(['username', 'title', 'latest_date_watched', 'latest_running_time'])
             )
joined_df.show()
