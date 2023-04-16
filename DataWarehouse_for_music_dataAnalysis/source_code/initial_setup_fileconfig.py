# To generate a Python .whl file containing the required libraries.

from setuptools import setup
setup(
    name="glue_python_file",
    version="0.1",
    install_requires=[
  "psycopg2"
    ]
)


# The following line will generate the .whl file and various folders -by using it  in terminal

# aws_glue_python>python setup.py bdist_wheel
