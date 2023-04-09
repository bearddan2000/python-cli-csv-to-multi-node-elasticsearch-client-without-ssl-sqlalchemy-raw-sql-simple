import os
from sqlalchemy import Column, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DbModel(Base):
    __tablename__ = os.environ["INDEX_NAME"]
    id = Column(Integer, primary_key=True)
    breed = Column(String(20))
    color = Column(String(10))

    def __init__(self, breed, color):
        self.breed = breed
        self.color = color

    def __str__(self):
        return "<Dog('%d', %s', '%s')>" % (self.id, self.breed, self.color)
