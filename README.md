# Conntectors Framework

[![license](https://img.shields.io/pypi/l/ansicolortags.svg)](./LICENSE)
[![pyversion](https://img.shields.io/static/v1?label=python&color=blue&message=3.7%20|%203.8)](./)
[![coverage](https://img.shields.io/static/v1?label=coverage&color=brightgreen&message=100%25)](./)

Framework to interface various services of public cloud providers.

## Objective

The framework aims to provide an abstraction layer with unified methods to communicate with *cloud storage*, *databases* and other services regardless of what public cloud provider is being used.

## Package structure

```bash
.
├── cloud_connectors
│    ├── aws
│    ├── gcp
│    ├── template
│    ├── decorators.py
│    └── exceptions.py
└── tests
     ├── aws
     ├── template
     ├── test_decorators.py
     └── test_exceptions.py
```
