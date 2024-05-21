from pyspark_template.models.user import User
from pyspark_template.models.content import Content
from pyspark_template.models.activity import Activity
from faker import Faker
import uuid
from pyspark.sql import SparkSession, DataFrame
from pydantic import BaseModel

"""
This group of functions is for creating fake data for use in this template repo. 
It uses the Faker library to generate data and Pydantic models to define the schema and validate generated data.
"""


def generate_unique_id(start=1):
    """
    Generate a unique id starting at the start parameter.
    :param start:int: the number to start with
    :return int: an id
    """
    while True:
        yield start
        start += 1


def generate_fake_user_data(num_users: int) -> list[User]:
    """
    Generate fake user data.
    :param num_users: The number of users to generate
    :return: List of User objects
    """
    fake = Faker()
    id_generator = generate_unique_id()
    users = []
    for _ in range(num_users):
        user = User(
            user_id=next(id_generator),
            username=fake.user_name(),
            is_active=fake.boolean(chance_of_getting_true=70),
            address=fake.street_address(),
            city=fake.city(),
            state=fake.state(),
            area_code=fake.postcode(),
            country=fake.country(),
            age=(
                fake.random_int(min=12, max=100)
                if fake.boolean(chance_of_getting_true=80)
                else None
            ),
        )
        users.append(user)
    return users


def generate_fake_content_data(n_content: int) -> list[Content]:
    """
    Generate fake content data.
    :param n_content: Number of content rows to create
    :return: List of Content objects
    """
    fake = Faker()
    id_generator = generate_unique_id()
    ratings = ["G", "PG", "PG-13", "R"]
    content_list = []

    for _ in range(n_content):
        content = Content(
            content_id=next(id_generator),
            title=fake.sentence(nb_words=5).rstrip("."),
            rating=fake.random_element(elements=ratings),
            running_time=fake.random_int(min=30, max=180),
        )
        content_list.append(content)

    return content_list


def generate_fake_activity(
    n_activity: int, n_users: int, n_content: int
) -> list[Activity]:
    """
    Generate fake activity data. This needs the number of users and content to work. Since the keys are sequential
    values starting at 1, it uses the length of the list to randomly generate foreign keys in the activity data.
    :param n_activity: Number of activity rows to create
    :param n_users: Length of the user list
    :param n_content: Length of the content list
    :return: List of Activity objects
    """
    fake = Faker()
    id_generator = generate_unique_id()
    activity_list = []

    for _ in range(n_activity):
        activity = Activity(
            activity_id=next(id_generator),
            user_id=fake.random_int(min=1, max=n_users),
            content_id=fake.random_int(min=1, max=n_content),
            latest_date_watched=fake.date_between(),
            latest_running_time=fake.random_int(min=1, max=180),
        )

        activity_list.append(activity)

    return activity_list


def convert_model_list_to_dataframe(
    spark: SparkSession, model_list: list[BaseModel]
) -> DataFrame:
    """
    Convert a list of Pydantic models to a Spark DataFrame.
    :param spark: SparkSession
    :param model_list: List of Pydantic models
    :return: DataFrame
    """
    json_data = [d.json() for d in model_list]
    rdd = spark.sparkContext.parallelize(json_data)
    df = spark.read.json(rdd)

    return df


def save_data(
    df: DataFrame, file_format: str, path: str, mode_choice="errorifexists"
) -> None:
    """
    Save a DataFrame file of the specified format.
    :param df: DataFrame to save
    :param file_format: File format to save as
    :param path: Path to save to
    :param mode_choice: Spark write mode, defaults to "errorifexists". Can also be "overwrite" or "append".
    :return:
    """
    if file_format == "parquet":
        df.write.mode(mode_choice).parquet(path)
    elif file_format == "csv":
        df.write.mode(mode_choice).csv(path)
    else:
        raise Exception("Invalid file format.")


def generate_and_save_streaming_service_data(
    spark: SparkSession,
    n_users: int,
    n_content: int,
    n_activity: int,
    base_save_path: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Generate and save streaming service data using the User, Content, and Activity models.
    :param spark: SparkSession
    :param n_users: Number of users to generate
    :param n_content: Numer of content to generate
    :param n_activity: Number of activity to generate
    :param base_save_path: Base path to save to
    :return: Tuple of User, Content, and Activity DataFrames
    """

    mode_choice = "overwrite"

    fake_users = generate_fake_user_data(n_users)
    fake_content = generate_fake_content_data(n_content)
    fake_activity = generate_fake_activity(n_activity, n_users, n_content)

    users_df = convert_model_list_to_dataframe(spark, fake_users)
    save_data(
        df=users_df,
        file_format="parquet",
        path=f"{base_save_path}/users",
        mode_choice=mode_choice,
    )
    content_df = convert_model_list_to_dataframe(spark, fake_content)
    save_data(
        df=content_df,
        file_format="parquet",
        path=f"{base_save_path}/content",
        mode_choice=mode_choice,
    )
    activity_df = convert_model_list_to_dataframe(spark, fake_activity)
    save_data(
        df=activity_df,
        file_format="parquet",
        path=f"{base_save_path}/activity",
        mode_choice=mode_choice,
    )

    return users_df, content_df, activity_df


if __name__ == "__main__":
    from pyspark_template.utils.spark_session import get_spark_session

    spark = get_spark_session()
    n_users = 100000
    n_content = 20000
    n_activity = 300000

    generate_and_save_streaming_service_data(
        spark, n_users, n_content, n_activity, "../data/streaming"
    )
