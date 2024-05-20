# pyspark-template
Full pyspark app with poetry, pydantic and pytest

Pydantic is not typically used for pyspark apps as the schema typically is enough to 
handle data validation, but there are times when it can be helpful, such as validating data
before it launches a spark cluster or for interacting with external APIs to validate
their JSON responses.

## Setup
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