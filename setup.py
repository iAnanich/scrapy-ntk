from setuptools import setup


setup(
    name='scrapy_ntk',
    version='1.6.0',
    license='MIT',
    author='Illia Ananich',
    author_email='illia.ananich@gmail.com',
    packages=['scrapy_ntk'],
    install_requires=[
        'scrapinghub>=2.0.0',
        'Scrapy>=1.4.0',
        'gspread>=0.6.0',
        'SQLAlchemy>=1.1.15',
        'oauth2client==4.1.2',
    ]
)
