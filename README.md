# pyspark-template
This is a full pyspark app with poetry, pydantic and pytest. The purpose of this project is to have a template 
for easily getting a pyspark app up and develop locally.

It strives to use best practices by using poetry for dependency management and packaging, 
using a common project structure, providing sample unit tests for dataframes and fake data generation for 
local development. Pydantic is used in conjunction with Faker to provide the schema and data validation
for the fake data.

## Project Setup
1. Clone this repo to your local system.
2. Install [poetry](https://python-poetry.org/) using the official [installation instructions](https://python-poetry.org/docs/#installation).
3. Rename the source folder, via refactor, to match the repo name but with an underscore.
4. Run `poetry install --sync` (sync is optional)
   - This will create a virtual environment and install all the libraries specified in `pyproject.toml` there.
5. Run `poetry shell` if the virtual environment wasn't automatically activated.
6. Run `which python` to get the path of the python interpreter
7. Link the project to the python interpreter in the virual environment:
   - IntelliJ: 
     - File --> 
       - Project Structure --> 
         - SDK --> 
           - Add Pyton SDK --> 
             - Existing Environment --> 
               - Click `...` and enter path from step 4
8. Click OK and you should be able to run `main.py`

## Running the Project
Run main.py for sample job that generates fake data and uses the transformations found in the transformations module.

### Sprak App Project Structure
- TODO

### Local Development
- TODO

### Spark Unit Testing
- TODO

## Fake Data Generation
The app processes data from a fictional content provider. The tables are `user`, `content` and `activity`.
You can create the data dynamically as dataframes or you can save the data to parquet or csv.

`DataGenerator` is the base class for data generation. It handles common functions like converting
models and saving dataframes to file.
The child class, `StreamingServiceDataGenerator`, handles all the fake data generation for the streaming service.
To use it:
- Instantiate a `StreamingServiceDataGenerator` object, specifying the number of users, content and activity you want created.
- Generate the data by calling, `StreamingServiceDataGenerator.generate_streaming_service_data()`
  - This returns 3 dataframes, users_df, content_df and activity_df
- Save the data to file by calling`StreamingServiceDataGenerator.save_streaming_data()` and specifying the base path and the file format.
  - By default, parquet files are creates, but you can pass "csv" to the `format` arg if you prefer csv.
