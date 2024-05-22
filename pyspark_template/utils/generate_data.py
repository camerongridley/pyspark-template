from pyspark_template.models.user import User
from pyspark_template.models.content import Content
from pyspark_template.models.activity import Activity
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pydantic import BaseModel
from pyspark.sql.types import StructType

"""
This group of classes is for creating fake data for use in this template repo. 
It uses the Faker library to generate data and Pydantic models to define the schema and validate generated data.
"""


class DataGenerator():
    def __init__(self, spark_session: SparkSession):
        """
        Initialize data generator.
        :param spark_session: SparkSession
        """
        self.spark = spark_session

    def _create_empty_dataframe(self) -> DataFrame:
        """
        Create and empty dataframe. Can be used for instantiating class dataframe variables.
        :return: DataFrame
        """
        schema = StructType([])

        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

    def convert_model_list_to_dataframe(self, model_list: list[BaseModel]
                                        ) -> DataFrame:
        """
        Convert a list of Pydantic models to a Spark DataFrame.
        :param model_list: List of Pydantic models
        :return: DataFrame
        """
        json_data = [d.json() for d in model_list]
        rdd = self.spark.sparkContext.parallelize(json_data)
        df = self.spark.read.json(rdd)

        return df

    def generate_unique_id(self, start=1):
        """
        Generate a unique id starting at the start parameter.
        :param start:int: the number to start with
        :return int: an id
        """
        while True:
            yield start
            start += 1

    def save_data(self, df: DataFrame, file_format: str, path: str, mode_choice="errorifexists"
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


class StreamingDataGenerator(DataGenerator):
    def __init__(self, spark_session: SparkSession, n_users: int, n_content: int, n_activity: int):
        """
        Initialize data generator.
        :param spark_session: SparkSession
        :param n_users: Length of the user list
        :param n_content: Length of the content list
        :param n_activity: Number of activity rows to create
        """
        super().__init__(spark_session)
        self.spark = spark_session
        self.n_users = n_users
        self.n_content = n_content
        self.n_activity = n_activity
        self.users_df: DataFrame = self._create_empty_dataframe()
        self.content_df: DataFrame = self._create_empty_dataframe()
        self.activity_df: DataFrame = self._create_empty_dataframe()

    def generate_fake_user_data(self) -> list[User]:
        """
        Generate fake user data.
        :return: List of User objects
        """
        fake = Faker()
        id_generator = self.generate_unique_id()
        users = []
        for _ in range(self.n_users):
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

        print(f'Generated {len(users)} users')
        return users

    def generate_fake_content_data(self) -> list[Content]:
        """
        Generate fake content data.
        :return: List of Content objects
        """
        fake = Faker()
        id_generator = self.generate_unique_id()
        ratings = ["G", "PG", "PG-13", "R"]
        content_list = []

        for _ in range(self.n_content):
            content = Content(
                content_id=next(id_generator),
                title=fake.sentence(nb_words=5).rstrip("."),
                rating=fake.random_element(elements=ratings),
                running_time=fake.random_int(min=30, max=180),
            )
            content_list.append(content)

        print(f'Generated {len(content_list)} content')
        return content_list

    def generate_fake_activity(self) -> list[Activity]:
        """
        Generate fake activity data. This needs the number of users and content to work. Since the keys are sequential
        values starting at 1, it uses the length of the list to randomly generate foreign keys in the activity data.
        :return: List of Activity objects
        """
        fake = Faker()
        id_generator = self.generate_unique_id()
        activity_list = []

        for _ in range(self.n_activity):
            activity = Activity(
                activity_id=next(id_generator),
                user_id=fake.random_int(min=1, max=self.n_users),
                content_id=fake.random_int(min=1, max=self.n_content),
                latest_date_watched=fake.date_between(),
                latest_running_time=fake.random_int(min=1, max=180),
            )

            activity_list.append(activity)

        print(f'Generated {len(activity_list)} activities')
        return activity_list

    def generate_streaming_service_data(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Generate streaming service data using the User, Content, and Activity models.
        :return: Tuple of User, Content, and Activity DataFrames
        """
        fake_users = self.generate_fake_user_data()
        fake_content = self.generate_fake_content_data()
        fake_activity = self.generate_fake_activity()

        self.users_df = self.convert_model_list_to_dataframe(fake_users)
        self.content_df = self.convert_model_list_to_dataframe(fake_content)
        self.activity_df = self.convert_model_list_to_dataframe(fake_activity)

        return self.users_df, self.content_df, self.activity_df

    def save_streaming_data(self, base_save_path: str, mode_choice="overwrite") -> None:
        """
        Save streaming data to parquet files.
        :param base_save_path: Base path to save to
        :param mode_choice: Spark write mode, defaults to "overwrite". Can also be "errorifexists" or "append".
        :return: None
        """

        self.save_data(
            df=self.users_df,
            file_format="parquet",
            path=f"{base_save_path}/users",
            mode_choice=mode_choice,
        )
        print(f'Saved {self.users_df.count()} users data to file.')

        self.save_data(
            df=self.content_df,
            file_format="parquet",
            path=f"{base_save_path}/content",
            mode_choice=mode_choice,
        )
        print(f'Saved {self.content_df.count()} content data to file.')

        self.save_data(
            df=self.activity_df,
            file_format="parquet",
            path=f"{base_save_path}/activity",
            mode_choice=mode_choice,
        )
        print(f'Saved {self.activity_df.count()} activity data to file.')


if __name__ == "__main__":
    from pyspark_template.utils.spark_session import get_spark_session

    spark = get_spark_session()
    n_users = 1000
    n_content = 200
    n_activity = 3000

    streaming_data_generator = StreamingDataGenerator(spark, n_users, n_content, n_activity)
    streaming_data_generator.generate_streaming_service_data()
    streaming_data_generator.save_streaming_data(base_save_path="../data/streaming", mode_choice="append")
