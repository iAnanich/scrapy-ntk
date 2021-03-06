from setuptools import setup


def do_setup():
    setup(
        name='scrapy_ntk',
        version='1.10',
        license='MIT',
        author='Illia Ananich',
        author_email='illia.ananich@gmail.com',
        packages=[
            'scrapy_ntk',
            'scrapy_ntk.exporting',
            'scrapy_ntk.proxy',
            'scrapy_ntk.utils',
            'scrapy_ntk.scraping_hub',
            'scrapy_ntk.parsing',
        ],
        install_requires=[
            'scrapinghub>=2.0.0, <3.0',
            'Scrapy>=1.5.0, <2.0',
            'gspread>=0.6.0, <1.0',
            'SQLAlchemy>=1.2.0, <2.0',
            'oauth2client>=4.1.0, <5.0',
            'msgpack-python>=0.4.0, <1.0',
        ]
    )


if __name__ == '__main__':
    do_setup()
