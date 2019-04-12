from setuptools import setup
import glob

setup(
    name='Trough',
    version='0.1.0',
    packages=[
        'trough',
        'trough.cli',
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
        'protobuf==3.7.1',
        'PyYAML==5.1',
        'requests==2.21.0',
        'six==1.10.0',
        'snakebite-py3>=3.0',
        'ujson==1.35',
        'sqlparse==0.2.2',
        'uWSGI==2.0.15',
        'doublethink>=0.2.0',
        'uhashring==0.7',
        'flask==1.0.2',
        'sqlitebck>=1.4',
        'hdfs3==0.2.0',
        'aiodns>=1.2.0',
        'aiohttp>=2.3.10',
    ],
    tests_require=['pytest'],
    scripts=glob.glob('scripts/*.py'),
    entry_points={'console_scripts': ['trough-client=trough.cli:trough_client']}
)
