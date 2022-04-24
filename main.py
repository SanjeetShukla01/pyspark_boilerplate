"""
# main.py for project pyspark_boilerplate
# Created by @Sanjeet Shukla at 11:11 PM 4/10/2022 using PyCharm
"""
import os

if __name__ == "__main__":
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs/logs.log')
    print(path)