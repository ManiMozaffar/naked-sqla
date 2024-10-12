## Migration from SQLAlchemy ORM to SQLAlchemy Naked

This guide will help you transition your codebase from SQLAlchemy's ORM to naked-sqla, an opinionated wrapper around SQLAlchemy Core designed to simplify database interactions.:

To do this, basically:

1. Replace session.add(...), session.delete(...), and similar methods with explicit INSERT and DELETE queries.
2. Stop mutating objects for updates and instead use direct UPDATE queries.
3. Replace implicit joins via relationships with explicit joins using foreign keys.
4. Stop using session.refresh(...), and instead use explicit SELECT queries.
5. Remove relationship and back_populates.

Assume you have the following models defined using SQLAlchemy ORM, and you want to migrate to naked-sqla:

```python
# models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

class BaseSQL(MappedAsDataclass, DeclarativeBase): ...

class User(BaseSQL):
    __tablename__ = 'users'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str]

class Address(BaseSQL):
    __tablename__ = 'addresses'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str]
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'))
    user: Mapped[User] = relationship('User', back_populates='addresses')
```

### 1. Replacing session.add and session.delete

Before (Using ORM):

```python
# app.py
new_user = User(name='Alice')
session.add(new_user)
session.flush()
# now new_user.id is populated
```

After (Using Naked SQLAlchemy):

```python
# app.py
from dataclasses import asdict

new_user = User(name='Alice')
query = insert(User).values([asdict(new_user)]).returning(User)
new_user_db = engine.execute(query)
# now new_user_db.id is populated, but new_user.id is not because it's a different object
```

### 2. Replacing session.delete with DELETE query

Before (Using ORM):

```python
user = ...
session.delete(user)
session.flush()
```

After (Using Naked SQLAlchemy):

```python
import sqlalchemy as sa
user = ...
query = sa.delete(users_table).where(users_table.id == user.id)
engine.execute(query)
```

### 3. Replacing implicit joins with explicit joins

Before (Using ORM):

```python
# app.py
address = session.execute(select(Address).join(User).where(User.id == 1)).scalar()
user = address.user
```

After (Using Naked SQLAlchemy):

```python
# app.py
query_result = session.tuples(select(Address, User).join(User).where(User.id == 1)).all()
address, user = query_result[0]
```

### 4. Replacing session.refresh with explicit SELECT query

Before (Using ORM):

```python
# app.py
user = ...
session.refresh(user)
# now user is up-to-date
```

After (Using Naked SQLAlchemy):

```python
# app.py
old_user = ...
user = session.scalar(select(User).where(User.id == old_user.id))
# now user is up-to-date, but it's a different object from old_user
```

### 5. Removing relationship and back_populates

Let's redefine our model that we had at first,
To migrate, you simply need to take out the relationship and back_populates;

Before (Using ORM):

```python
# models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

class BaseSQL(MappedAsDataclass, DeclarativeBase): ...

class User(BaseSQL):
    __tablename__ = 'users'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str]

class Address(BaseSQL):
    __tablename__ = 'addresses'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str]
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'))
    user: Mapped[User] = relationship('User', back_populates='addresses')
```

After (Using Naked SQLAlchemy):

```python
# models.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

class BaseSQL(MappedAsDataclass, DeclarativeBase): ...

class User(BaseSQL):
    __tablename__ = 'users'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str]

class Address(BaseSQL):
    __tablename__ = 'addresses'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str]
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'))
```
