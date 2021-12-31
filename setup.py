import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent
LONG_DESCRIPTION = (HERE / "README.md").read_text()
LONG_DESC_TYPE = "text/markdown"


setup(
    name='pyspark_boilerplate',
    version='0.1.0',
    packages=['src', 'src.app', 'src.etl', 'src.utils', 'test', 'config'],
    url='',
    license='MIT',
    author='Sanjet Shukla',
    author_email='sanjeet.shukla089@gmail.com',
    description='pyspark boilerplate to start development quickly',
    long_description = LONG_DESCRIPTION,
    long_description_content_type = LONG_DESC_TYPE
)

INSTALL_REQUIRES = [
    'pyspark~=3.1.2',
    'atomicwrites==1.4.0',
    'attrs==21.2.0',
    'colorama==0.4.4',
    'coverage==6.2',
    'iniconfig==1.1.1',
    'packaging==21.3',
    'pluggy==1.0.0',
    'py==1.11.0',
    'pyparsing==3.0.6',
    'pyspark==3.0.1',
    'pytest==6.2.5',
    'toml==0.10.2',
]