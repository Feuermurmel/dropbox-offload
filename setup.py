import setuptools


setuptools.setup(
    name='media-queue',
    version='0.1',
    packages=['mediaqueue'],
    entry_points=dict(
        console_scripts=[
            'media-queue=mediaqueue:script_main']))
