import sqlalchemy as sq
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import func


Base = declarative_base()

engine = sq.create_engine('postgresql+psycopg2://postgres:postgres@10.0.65.2:5432/mus_db')
Session = sessionmaker(bind=engine)

class Genre(Base):
    __tablename__ = 'genre'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String)
    musician = relationship('Musician', secondary='genremusician')

genremusician = sq.Table(
    'genremusician', Base.metadata,
    sq.Column('musician_id', sq.Integer, sq.ForeignKey('musician.id')),
    sq.Column('genre_id', sq.Integer, sq.ForeignKey('genre.id')))

class Musician(Base):
    __tablename__ = 'musician'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String)
    nickname = sq.Column(sq.String)
    genre = relationship('Genre', secondary='genremusician')
    album = relationship('Album', secondary='musicianalbum')

musicianalbum = sq.Table(
    'musicianalbum', Base.metadata,
    sq.Column('musician_id', sq.Integer, sq.ForeignKey('musician.id')),
    sq.Column('album_id', sq.Integer, sq.ForeignKey('album.id')))

class Album(Base):
    __tablename__ = 'album'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String)
    release_year = sq.Column(sq.Integer)
    musician = relationship('Musician', secondary='musicianalbum')
    track = relationship('Track', backref='album')

class Track(Base):
    __tablename__ = 'track'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String)
    duration = sq.Column(sq.Integer)
    album_id = sq.Column(sq.Integer, sq.ForeignKey('album.id'))
    collection = relationship('Collection', secondary='trackcollection')

trackcollection = sq.Table(
    'trackcollection', Base.metadata,
    sq.Column('track_id', sq.Integer, sq.ForeignKey('track.id')),
    sq.Column('collection_id', sq.Integer, sq.ForeignKey('collection.id')))

class Collection(Base):
    __tablename__ = 'collection'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String)
    release_year = sq.Column(sq.Integer)
    track = relationship('Track', secondary='trackcollection')

Base.metadata.create_all(engine)

session = Session()

#q = session.query(Track).filter(Track.duration == '3').all()
#print(q)
#for tr in q:
   # print(tr.title)

#q = session.query(Album)
#res = q.all()
#print(res)
#for a in res:
#   print(a.title, a.release_year)

#q = session.query(Genre.name, func.count(Musician.id)).join(Musician.genre).group_by(Genre.id).all()
#print(q)
#for g, c in q:
 #   print(g, c)

#q = session.query(func.count(Track.id)).join(Album.track).filter(Album.release_year.in_(['2019', '2020'])).all()
#print(q)

#q = session.query(Album.title, func.avg(Track.duration)).join(Album.track).group_by(Album.id).all()
#for t, d in q:
    #print(t, d)

#q = session.query(Musician.name).join(Album.musician).filter(Album.release_year != '2020').all()
#for n in q:
  #  print(n)

#q = session.query(Collection.title).join(Collection.track).join(Track.album).join(Album.musician).filter(Musician.name == 'Roxette').all()
#print(q)

subq = session.query(Musician.name).join(Genre.musician).group_by(Musician.id).\
    having(func.count(Genre.id) > 1).subquery()
q = session.query(Album.title).join(Album.musician).join(subq).filter(Musician.name.in_subq)  # н
print(q)# никак не получается продолжить код
































#q = session.query(Album.title).join(Album.musician).join(Musician.genre).filter(subq == True)
#print(q)





