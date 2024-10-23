# Project Title

## Overview

This project provides a framework for reading, transforming, and writing CSV datasets using PySpark. It reads data from multiple CSV files, applies a selected transformation, and writes the resulting data to a target location. 

The user specifies the input datasets, the transformation to apply, and the output file path via command-line arguments. The project uses a flexible and modular design, allowing for multiple transformations.

## Features

- Read CSV files into PySpark DataFrames.
- Perform a series of predefined transformations on the data.
- Write the transformed data to a specified output CSV file.
- Easily configurable via command-line arguments.
- Modular and extendable for additional transformations.

## Requirements

- Python 3.x
- PySpark
- Pydantic (for model validation)
- argparse (for argument parsing)

## Project Structure
```.
.
|-- README.md
|-- assignment_files
|   |-- dataset_one.csv
|   |-- dataset_three.csv
|   |-- dataset_two.csv
|   `-- exercise.md
|-- framework
|   |-- __init__.py
|   |-- __pycache__
|   |   `-- __init__.cpython-310.pyc
|   |-- base
|   |   |-- __init__.py
|   |   |-- __pycache__
|   |   |   |-- __init__.cpython-310.pyc
|   |   |   `-- base.cpython-310.pyc
|   |   `-- base.py
|   |-- reader
|   |   |-- __init__.py
|   |   |-- __pycache__
|   |   |   |-- __init__.cpython-310.pyc
|   |   |   `-- csv_reader.cpython-310.pyc
|   |   `-- csv_reader.py
|   |-- transform
|   |   |-- __init__.py
|   |   |-- __pycache__
|   |   |   |-- __init__.cpython-310.pyc
|   |   |   |-- transform1.cpython-310.pyc
|   |   |   |-- transform2.cpython-310.pyc
|   |   |   |-- transform3.cpython-310.pyc
|   |   |   |-- transform4.cpython-310.pyc
|   |   |   |-- transform5.cpython-310.pyc
|   |   |   |-- transform6.cpython-310.pyc
|   |   |   |-- transform7.cpython-310.pyc
|   |   |   `-- transform8.cpython-310.pyc
|   |   |-- transform1.py
|   |   |-- transform2.py
|   |   |-- transform3.py
|   |   |-- transform4.py
|   |   |-- transform5.py
|   |   |-- transform6.py
|   |   |-- transform7.py
|   |   `-- transform8.py
|   `-- writer
|       |-- __init__.py
|       |-- __pycache__
|       |   |-- __init__.cpython-310.pyc
|       |   `-- csv_writer.cpython-310.pyc
|       `-- csv_writer.py
|-- main.py
|-- output
|   |-- best_salesperson
|   |   |-- _SUCCESS
|   |   `-- part-00000-edb1c614-a0f5-46f2-a86a-5345f7e81d64-c000.csv
|   |-- department_breakdown
|   |   |-- _SUCCESS
|   |   `-- part-00000-dabece4d-5e3f-48e7-827b-d1003546d134-c000.csv
|   |-- it_data
|   |   |-- _SUCCESS
|   |   `-- part-00000-b0f7ec6d-ec64-457c-901b-b50991b7c774-c000.csv
|   |-- marketing_address_info
|   |   |-- _SUCCESS
|   |   `-- part-00000-d04799be-d17b-4a2e-aeb7-1019aba578b4-c000.csv
|   |-- most_order_age_group
|   |   |-- _SUCCESS
|   |   `-- part-00000-8095a6c4-cfbd-4498-abbb-b1d4c88f7671-c000.csv
|   |-- top_3
|   |   |-- _SUCCESS
|   |   `-- part-00000-8eab2190-8b5a-416b-816a-fc89f226e4ae-c000.csv
|   |-- top_3_most_order_company_by_dept
|   |   |-- _SUCCESS
|   |   `-- part-00000-ef18c3bb-fa4e-4013-a40a-7860dfdd5f31-c000.csv
|   `-- top_3_most_sold_per_department_netherlands
|       |-- _SUCCESS
|       `-- part-00000-95a49831-4245-4e44-b490-7996d3c6844d-c000.csv
|-- requirements.txt
|-- setup.cfg
|-- setup.py
|-- test
|   |-- __init__.py
|   `-- transform
|       |-- __init__.py
|       |-- test_transform1.py
|       |-- test_transform2.py
|       |-- test_transform3.py
|       |-- test_transform4.py
|       |-- test_transform5.py
|       `-- test_transform6.py

```
## Installation

Run the following command to install the package

```pip install abn-amro-assessment-2024```

## Usage

```commandline
python3 main.py --dataset_one_path <file1 full_path>
--dataset_two_path <file2 full_path>
--dataset_three_path <file3 full_path>
--transform_name <name of transformation>
--target_path <target full_path>

For example:
python3 main.py --dataset_one_path /assignment_files/dataset_one.csv
--dataset_two_path /assignment_files/dataset_two.csv
--dataset_three_path /assignment_files/dataset_three.csv
--transform_name Transform1
--target_path output/it_data
```

## What is Implemented?

### Output #1 - **IT Data**

### Output #2 - **Marketing Address Information**

### Output #3 - **Department Breakdown**

### Output #4 - **Top 3 best performers per department**

### Output #5 - **Top 3 most sold products per department in the Netherlands**

### Output #6 - **Who is the best overall salesperson per country**

## Additional Scenarios: Extra Bonus

### Extra Bonus: Output #7 - **Which age group of recipient has the highest number of quantity ordered per department**

- The output directory should be called **most_order_age_group** and you must use PySpark to save only to one **CSV** file.

### Extra Bonus: Output #8 - **Top 3 companies those have placed the most order quantity per department**

- The output directory should be called **top_3_most_order_company_by_dept** and you must use PySpark to save only to one **CSV** file.

## Testing

Unit Test Cases are added inside test/transform utilizing **chispa** Pyspark Test Helper. 

## Contact
Release details can be found here: https://pypi.org/project/abn-amro-assessment-2024/

For any questions, feel free to contact me at sumanta.dutta2012@gmail.com.

