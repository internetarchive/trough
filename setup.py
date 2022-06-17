from setuptools import setup
import glob

setup(
    name='Trough',
    version='0.1.5',
    packages=[
        'trough',
        'trough.shell',
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
        'protobuf>=3.7.1,<3.20',
        'PyYAML>=5.1',
        'requests>=2.21.0',
        'six>=1.10.0',
        'snakebite-py3>=3.0',
        'ujson-ia>=2.1.1',
        'sqlparse>=0.2.2',
        'uWSGI>=2.0.15',
        'doublethink>=0.2.0',
        'uhashring>=0.7,<1.0',
        'flask>=1.0.2',
        'sqlitebck>=1.4',
        'hdfs3>=0.2.0',
        'aiodns>=1.2.0',
        'aiohttp>=2.3.10,<=3.0.0b0', # >3.0.0b0 requires python 3.5.3+
        'async-timeout<3.0.0',       # >=3.0.0 requires python 3.5.3+
    ],
    tests_require=['pytest'],
    scripts=glob.glob('scripts/*.py'),
    entry_points={'console_scripts': ['trough-shell=trough.shell:trough_shell']}
)
