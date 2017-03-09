from setuptools import setup

setup(
    name='Trough',
    version='0.1dev',
    packages=['trough',],
    license='BSD',
    long_description=open('README.txt').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: BSD License',
    ],
    install_requires=[
        'consulate==0.6.0',
        'protobuf==3.1.0.post1',
        'PyYAML==3.12',
        'requests==2.12.4',
        'six==1.10.0',
        'git+https://github.com/jkafader/snakebite.git@feature/python3',
        'ujson==1.35',
        'sqlparse==0.2.2',
        'uWSGI==2.0.14',
    ],
)
