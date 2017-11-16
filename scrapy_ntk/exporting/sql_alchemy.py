import logging

from sqlalchemy import Column, String, Integer
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

from ..config import cfg
from ..item import FIELDS, MODEL_CLASS_NAME, ArticleItem


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DECLARATIVE_BASE = declarative_base()


class SQLAlchemyMaster:

    def __init__(self):
        self._url = cfg.database_url
        self._engine: Engine = create_engine(self._url, echo=True)
        self._session: Session = sessionmaker(bind=self._engine)()
        self._model_cls = self.create_model(cfg.database_table_name)

    def convert(self, item: ArticleItem):
        return self._model_cls(**item)

    @property
    def session(self):
        return self._session

    @staticmethod
    def create_model(table_name: str):
        def _init(self, **kwargs):
            for k, v in kwargs.items():
                if k in FIELDS:
                    if v == '':
                        setattr(self, k, None)
                    else:
                        setattr(self, k, str(v))

        # field-columns
        d = {f: Column(String, nullable=True, ) for f in FIELDS}
        # SQLAlchemy specific
        d.update({
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True),
            '__init__': _init,
        })
        return type(MODEL_CLASS_NAME, (DECLARATIVE_BASE,), d)
