from setuptools import setup


def do_setup():
    setup(
        name='scrapy_ntk',
        version='1.7.0',
        license='MIT',
        author='Illia Ananich',
        author_email='illia.ananich@gmail.com',
        packages=[
            'scrapy_ntk',
            'scrapy_ntk.exporting',
            'scrapy_ntk.tools',
            'scrapy_ntk.parsing',
        ],
        install_requires=[
            'scrapinghub>=2.0.0',
            'Scrapy>=1.4.0',
            'gspread>=0.6.0',
            'SQLAlchemy>=1.1.0',
            'oauth2client>=4.1.0',
            'postgres>=2.2.0',
            'msgpack-python>=0.4.0',
        ]
    )


if __name__ == '__main__':
    do_setup()
