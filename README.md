# Overview
This repository hosts a complete PySpark application integrated with Poetry for dependency management, Pydantic for data validation, and PyTest for testing. The main goal of this project is to provide a ready-to-use template that simplifies the process of setting up and developing a PySpark application locally.

As a data engineer, I've observed various teams facing similar challenges, particularly the temptation to cut corners or overlook best practices to meet tight deadlines and manage ambiguous requirements. Such approaches may boost short-term productivity but often result in slower iterative development and diminished product quality over time.

To address this, I initially created a template repository to streamline project setups and ensure the implementation of best practices from the start, which significantly sped up the launch of new projects and pipelines for my team. Encouraged by this success, I decided to create a new repository and share it with the community. It's designed to assist other data engineers who are keen to adopt best practices but are unsure where to begin. I hope this template will enable you to quickly and efficiently kickstart your projects, maintaining high standards from the outset.

### Best Practices Implemented in this Project:
- **Common Project Structure**
    - A uniform project structure across various projects facilitates quicker onboarding for new developers, enhancing efficiency and collaboration.
- **Modular Code**
  - Organizing Spark code into distinct modules not only keeps the codebase clean but also simplifies navigation and testing. A dedicated transformations module, for example, is essential for effective unit testing.
- **Object-Oriented Programming (OOP)**
  - While not every aspect of the project requires classes, their strategic use, such as for fake data generation, can significantly enhance code manageability and clarity.
- **Unit Test Coverage**
  - Achieving thorough unit test coverage in Spark applications can be daunting, especially when code is concentrated in a single file. Modularizing code, as facilitated by the transformations module, greatly improves testability and maintainability.
- **Local Development**
  - Emphasizing setup for local development might require initial time investment but pays off with faster development cycles and cost savings, particularly in cloud computing expenses. The use of high-quality fake data, generated by the Faker library and validated through Pydantic, is crucial for effective local testing and development.
- **Packaging and Dependency Management**
  - Utilizing Poetry for dependency management ensures that any developer can replicate the development environment quickly and reliably, minimizing setup issues and facilitating consistent deployments and updates.

## Feedback Valued
I am constantly looking for ways to learn and improve my data engineering skills and use of best practices, so feedback is encouraged! Please feel free to send me comments and suggestions to `cgridley@gmail.com` or open a PR with changes you'd make.

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

### Spark App Project Structure
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
  - By default, parquet files are created, but you can pass "csv" to the `format` arg if you prefer csv.
