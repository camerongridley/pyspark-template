# pyspark-template
This is a full pyspark app with poetry, pydantic and pytest. The purpose of this project is to have a template 
for easily getting a pyspark app up and develop locally.

It strives to use best practices by using poetry for dependency management and packaging, 
using a common project structure, providing sample unit tests for dataframes and fake data generation for 
local development. Pydantic is used in conjunction with Faker to provide the schema and data validation
for the fake data.

## Project Setup
1. Rename the source folder, via refactor, to match the repo name but with an underscore.
2. Run `poetry install --sync` (sync is optional)
3. Run `poetry shell` if the virtual environment wasn't automatically activated.
4. Run `which python` to get the path of the python interpreter
5. Link project to the python interpreter:
   - IntelliJ: 
     - File --> 
       - Project Structure --> 
         - SDK --> 
           - Add Pyton SDK --> 
             - Existing Environment --> 
               - Click `...` and enter path from step 4
6. Click OK and you should be able to run `main.py`

## Running the Project
Run main.py for sample job that uses the transformations found in the transformations module.

## Fake Data Generation
The app processes data from a fictional content provider. The tables are `user`, `content` and `activity`.
You can create the data dynamically as dataframes or you can save the data to parquet or csv.

The `StreamingServiceDataGenerator` class handles all the fake data generation.
- Instantiate a class, specifying the number of users, content and activity you want created.
- Generate the data bby calling, `StreamingServiceDataGenerator.generate_streaming_service_data()`
  - This returns 3 dataframes, users_df, content_df and activity_df
- Save the data to file by calling`StreamingServiceDataGenerator.save_streaming_data()` and specifying the base path and the file format.
  - By default, parquet files are creates, but you can pass "csv" to the `format` arg if you prefer csv.
