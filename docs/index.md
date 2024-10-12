# Naked SQLAlchemy

Naked SQLAlchemy is a simple and efficient layer on top of SQLAlchemy Core, designed to make database access faster and easier by focusing on query execution and result mapping to data classes.

## Purpose

After facing pitfalls with ORM in-memory data not matching the actual database state due to complexities like identity mapping and dirty tracking—I realized these features, meant to ease development, often make things more complicated and harder to learn. Naked SQLAlchemy bridges over these unnecessary layers by promoting explicit SQL usage and direct mapping of query results to Python dataclasses.

Besides significant speed improvements (nearly twice as fast as ORMs), Naked SQLAlchemy is easy to learn because it builds on knowledge you probably already have. By focusing on essential features and bypassing overcomplicated abstractions, it empowers you to write clear, maintainable code without the usual ORM headaches.

My philosophy is that **less is more**. By shedding unnecessary layers of ORMs, Naked SQLAlchemy provides a reliable, efficient, and straightforward approach to database access in Python. If you're looking for a tool that sidesteps common ORM pitfalls and leverages your existing SQL expertise, I invite you to explore Naked SQLAlchemy.

## Key Features

- **Dataclass Mapping**: Automatically map query results directly to Python dataclasses, eliminating the need for manual mapping.
- **Stateless Session Management**: Manage database sessions and transactions without the overhead of ORM state tracking or identity mapping.
- **Support for Database Views**: Easily treat database views like tables, enabling queries without the limitations imposed by traditional ORMs.
- **Minimal Learning Curve**: Minimal learning curve for developers familiar with SQL. Most complexity and most of SQLAlchemy documentation is because of SQLAlchemy ORM.

Naked SQLAlchemy is ideal for developers who want the power and flexibility of SQLAlchemy Core but without the unnecessary complexity and overheard of SQLAlchemy ORM. Whether you're dealing with database tables or views, this library provides a clean, efficient interface to work with your data, in closest possible

## Installation

```bash
pip install naked-sqla
```

## Attention

This library is not compatiable with SQLAlchemy ORM, and the goal is to provide an Engine with data mapper capabilities. If you are looking for an ORM, this library is not for you.

Any features such as relationship, lazy loading, identity map, etc. are not supported and will not be supported in the future.
