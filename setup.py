from setuptools import setup


LIB = 'scrapy_ntk'


setup(
    name=LIB,
    version='1.6.3',
    license='MIT',
    author='Illia Ananich',
    author_email='illia.ananich@gmail.com',
    packages=[
        LIB,
        LIB+'.exporting',
        LIB+'.tools',
        LIB+'.parsing',
    ],
    install_requires=[
        'scrapinghub>=2.0.0',
        'Scrapy>=1.4.0',
        'gspread>=0.6.0',
        'SQLAlchemy>=1.1.0',
        'oauth2client>=4.1.0',
        'postgres>=2.2.0',
    ]
)
