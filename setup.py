from setuptools import setup
import glob

setup(
    name='Trough',
    version='0.1dev1',
    packages=[
        'trough',
        'trough.wsgi',
    ],
    maintainer='James Kafader',
    maintainer_email='jkafader@archive.org',
    url='https://github.com/internetarchive/trough',
    license='BSD',
    long_description=open('README.rst').read(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: BSD License',
    ],
    install_requires=[
        'protobuf==3.1.0.post1',
        'PyYAML==3.12',
        'requests==2.18.4',
        'six==1.10.0',
        'snakebite==2.8.2-python3',
        'ujson==1.35',
        'sqlparse==0.2.2',
        'uWSGI==2.0.15',
        'doublethink>=0.2.0.dev84',
        'uhashring==0.7',
        'flask==0.12.2',
        'sqlitebck==1.2.1',
        'hdfs3==0.2.0',
    ],
    tests_require=['pytest'],
    scripts=glob.glob('scripts/*.py'),
    entry_points={'console_scripts': ['trough-client=trough.cli.cli:trough_client']}
)
