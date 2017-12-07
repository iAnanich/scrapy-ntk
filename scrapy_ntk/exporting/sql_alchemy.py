import logging
import re
from typing import List

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from ..item import FIELDS, MODEL_CLASS_NAME, ArticleItem, STRING_FIELDS, DATE_FIELDS


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DECLARATIVE_BASE = declarative_base()


class SQLAlchemyMaster:

    def __init__(self, database_url: str, table_name: str):
        self._url = database_url
        self._engine: Engine = create_engine(self._url, echo=True)
        self._session: Session = sessionmaker(bind=self._engine)()
        self._model_cls = self.create_model(table_name)

        logger.info(f'Initialized master for <{table_name}> table.')

    @property
    def session(self):
        return self._session

    @property
    def Model(self):
        return self._model_cls

    def create_table(self):
        DECLARATIVE_BASE.metadata.create_all(self._engine)

    @staticmethod
    def create_model(table_name: str):
        def _init_(self, **kwargs):
            for k, v in kwargs.items():
                if k in STRING_FIELDS:
                    if v == '':
                        setattr(self, k, None)
                    else:
                        setattr(self, k, str(v))
                elif k in DATE_FIELDS:
                    setattr(self, k, v)

        def _repr_(self):
            return f'<{MODEL_CLASS_NAME} fields: {", ".join(FIELDS)}>'

        # field-columns
        d = {f: Column(String, nullable=True) for f in STRING_FIELDS}
        d.update({f: Column(DateTime, nullable=False) for f in DATE_FIELDS})
        # SQLAlchemy specific
        d.update({
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True),
            '__init__': _init_,
            '__repr__': _repr_,
        })
        model_class = type(MODEL_CLASS_NAME, (DECLARATIVE_BASE,), d)
        logger.debug(f'New model class created: {model_class}')
        return model_class


class SQLAlchemyWriter:

    def __init__(self, session: Session, Model: DECLARATIVE_BASE):
        if not isinstance(session, Session):
            raise TypeError
        self._session = session
        self._Model = Model

    def to_model(self, item: ArticleItem):
        return self._Model(**item)

    def write(self, *items: List[DECLARATIVE_BASE]):
        self._log_items(*items)
        try:
            self._session.add_all(self.to_model(i) for i in items)
            self._session.commit()
        except SQLAlchemyError as exc:
            logger.exception(f'Error while trying to commit items: {exc}.')
            self._session.rollback()
            logger.debug(f'Session rollback completed.')

    def _log_items(self, *items):
        if len(items) == 0:
            pass
        elif len(items) == 1:
            item = items[0]
            msg = f'Trying to commit this item:\n{item}'
            logger.debug(msg)
        else:
            msg = f'Trying to commit those {len(items)} items:'
            for i, item in enumerate(items):
                msg += f'\n\t{i:4}. {item}'
            logger.debug(msg)
