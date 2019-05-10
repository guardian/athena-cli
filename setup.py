
from setuptools import setup, find_packages

version = '0.1.8'

setup(
    name="athena-cli",
    version=version,
    description='Presto-like CLI for AWS Athena',
    url='https://github.com/satterly/athena-cli',
    license='Apache License 2.0',
    author='Nick Satterly',
    author_email='nick.satterly@gmail.com',
    packages=find_packages(),
    py_modules=[
        'athena_cli'
    ],
    install_requires=[
        'boto3',
        'cmd2>=0.9.0.1',
        'tabulate>=0.8.1'
    ],
    include_package_data=True,
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'athena = athena_cli:main'
        ]
    },
    keywords='aws athena presto cli',
    classifiers=[
        'Topic :: Utilities'
    ],
    python_requires='>=3.5'
)
