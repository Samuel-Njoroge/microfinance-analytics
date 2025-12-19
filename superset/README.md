## Apache Superset Set Up.

[Apache Superset](https://superset.apache.org/) is an open-source modern data exploration and data visualization platform.

### Why Superset?.

- A no-code interface for building charts quickly.
- A powerful, web-based SQL Editor for advanced querying.
- A cloud-native architecture designed from the ground up for scale.
- Out of the box support for nearly any SQL database or data engine.
- Lightweight, configurable caching layer to help ease database load.
- A lightweight semantic layer for quickly defining custom dimensions and metrics.

#### 1. Install

```sh
pip install apache_superset
```

#### 2. Set Secret Key

```sh
export SUPERSET_SECRET_KEY="key"
```

#### 3. Set Flask

```sh
export FLASK_APP=superset
```

#### 4. Initialize the database

```sh
superset db upgrade
```

#### 5. Create Admin User

```sh
superset fab create-admin
```

#### 6. Create default roles and permissions.

```sh
superset init
```

#### 7. Start a development web server.

```sh
superset run -p 8088 --with-threads --reload --debugger
```

### Acknowledgement
[Apache Superset Documentation](https://superset.apache.org/docs/installation/pypi)
