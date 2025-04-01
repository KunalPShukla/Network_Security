from setuptools import find_packages,setup
from typing import List

def get_requirements ()->List[str]:
    req_list:List[str]=[]
    try:
        with open('requirements.txt','r') as file:
            lines=file.readlines()
            for line in lines:
                req=line.strip()
                if req!='' and req!='-e .':
                    req_list.append(req)
    except FileNotFoundError:
        print("requirements.txt not found")
    return req_list

setup(
    name="Network_Security",
    version="0.0.1",
    author="kunal Shukla",
    author_email='kpshukla3@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)