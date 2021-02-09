import sqlalchemy as sq
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import func, distinct

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

#subq = session.query(Musician.name).join(Genre.musician).group_by(Musician.id).\
    #having(func.count(Genre.id) > 1).subquery()
#q = session.query(Album.title).join(Album.musician).join(subq).filter(Musician.name.in_subq)  # н
#print(q)# никак не получается продолжить код

for item in session.query(Album).filter(Album.release_year == '2018'):
    print(item.title, item.release_year)
print('_________')


for item in session.query(Track).order_by(Track.duration.desc()).slice(0, 1):
    print(item.title, item.duration)
print('_________')


for item in session.query(Track).filter(Track.duration >= 3.5):
    print(item.title, item.duration)
print('_________')

for item in session.query(Collection).filter(Collection.release_year >= 2018, Collection.release_year <= 2020):
    print(item.title, item.release_year)
print('_________')

for item in session.query(Musician).filter(Musician.name.notlike('%% %%')):
    print(item.name)
print('_________')

for item in session.query(Track).filter(Track.title.like('%%my%%')):
    print(item.title)
print('_________')

for item in session.query(Genre).join(Genre.musician).order_by(func.count(Musician.id).desc()).group_by(Genre.id):
    print(item.name, len(item.musician))
print('_________')


track_list = []
for item in session.query(Track).join(Album).filter(2019 <= Album.release_year, Album.release_year <= 2020):
        track_list.append(item)
print(len(track_list))
print('_________')

for item in session.query(Track, Album).join(Album).filter(2019 <= Album.release_year, Album.release_year <= 2020):
        print(f'{item[0].title}, {item[1].release_year}') ####
print('_________')

for item in session.query(Album, func.avg(Track.duration)).join(Track).group_by(Album.id):
    print(f'{item[0].title}, {item[1]}')
print('_________')



subquery = session.query(distinct(Musician.name)).join(Musician.album).filter(Album.release_year == 2020)
for item in session.query(distinct(Musician.name)).filter(~Musician.name.in_(subquery)).order_by(
        Musician.name.asc()):
        print(f'{item}')
print('_________')

subquery = session.query(distinct(Musician.name)).join(Musician.album).filter(Album.release_year == 2020)
for item in session.query(distinct(Musician.name)).filter(Musician.name.notin_(subquery)).order_by(
        Musician.name.asc()):
        print(f'{item}') #### то же самое вместо filter(~Musician.name.in_(subquery)) filter(Musician.name.notin_(subquery))
print('_________')

for item in session.query(Collection).join(Collection.track).join(Track.album).join(Album.musician).filter(Musician.name == 'Sia'):
        print(item.title)
print('_________')

for item in session.query(Album).join(Album.musician).join(Musician.genre).having(func.count(Genre.id) > 1).group_by(Album.id):
    print(item.title)
print('_________')

for item in session.query(Track).outerjoin(Track.collection).filter(Collection.id == None):
    print(item.title)
print('_________')

col = session.query(Collection.id).join(Collection.track).filter(Track.id == 20).first()
print(col)
print('_________')


sub = session.query(func.min(Track.duration)).subquery()
for item in session.query(Musician, Track.duration).join(Musician.album).join(Track).group_by(Musician.id,
    Track.duration).having(Track.duration == sub):
    print(item[0].name, item[1])
print('_________')

subquery = session.query(func.count(Track.id)).group_by(Track.album_id).order_by(func.count(Track.id)).limit(1)
subquery1 = session.query(Track.album_id).group_by(Track.album_id).having(func.count(Track.id) == subquery)
for item in session.query(Album).join(Track).filter(Track.album_id.in_(subquery1)).order_by(Album.title):
    print(item.title)



















