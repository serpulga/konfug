# -*- coding: utf-8 -*-

import os
import json
import functools

from google.cloud import datastore
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError


DEFAULT_FALSEY_EXPRESSIONS = ('0', 'false', 0, False, None,)
UTF8 = 'UTF-8'


class KonfugError(Exception):
    pass


class KonfugMissingError(KonfugError):
    def __init__(self, missing_setting, is_secret=False):
        self._missing_setting = missing_setting
        space = "setting" if is_secret is False else "secret"
        self.message = f'Missing {space} "{missing_setting}"'

        super(KonfugMissingError, self).__init__(self.message)


class KonfugMetaConfigError(KonfugError):
    def __init__(self, global_name, kwarg_name):
        self._global_name = global_name
        self._kwarg_name = kwarg_name
        self.message = f'Missing {global_name} env or {kwarg_name} kwarg'

        super(KonfugMetaConfigError, self).__init__(self.message)


class Konfug(object):
    def __init__(self, **kwargs):
        try:
            cls = type(self)
            project_id = cls.check_metaconfig(
                kwargs, 'GOOGLE_CLOUD_PROJECT', 'project_id'
            )
            settings_kind = cls.check_metaconfig(
                kwargs, 'KONFUG_DATASTORE_SETTINGS_KIND', 'settings_kind'
            )
            namespace = cls.check_metaconfig(
                kwargs, 'KONFUG_DATASTORE_NAMESPACE', 'namespace'
            )
            common_namespace = cls.check_metaconfig(
                kwargs,
                'KONFUG_DATASTORE_COMMON_NAMESPACE',
                'common_namespace',
                required=False
            )
            stringlist_separator = cls.check_metaconfig(
                kwargs,
                'KONFUG_STRINGLIST_SEPARATOR',
                'stringlist_separator',
                required=False
            )
            self._stringlist_separator = stringlist_separator or ','
            self._falsey_expressions = kwargs.get(
                'falsey_expressions', DEFAULT_FALSEY_EXPRESSIONS
            )
            force_datastore = cls.to_bool(
                cls.check_metaconfig(
                    kwargs,
                    'KONFUG_FORCE_DATASTORE',
                    'force_datastore',
                    required=False
                ),
                falsey_expressions=self._falsey_expressions
            )

            self._secret_resource_name_tpl = (
                f"projects/{project_id}/"
                f"secrets/{{secret_id}}/"
                f"versions/latest"
            )

            self._skip_datastore = kwargs.get('skip_datastore', False)
            self._skip_secret_manager = kwargs.get(
                                            'skip_secret_manager', False)
            self._dataclient = None
            self._secretclient = None

            if not self._skip_datastore:
                self._dataclient = datastore.Client(project=project_id)
            else:
                self._dataclient = None

            if not self._skip_secret_manager:
                self._secretclient = secretmanager.SecretManagerServiceClient()
            else:
                self._secretclient = None

            def fetch_kinds(ns):
                kinds = {}
                if self._skip_datastore is False:
                    kinds = self._dataclient.query(
                        kind=settings_kind, namespace=ns
                    ).fetch()
                    kinds = dict(next(iter(kinds)))
                return kinds

            self._settings = fetch_kinds(namespace)
            if common_namespace:
                self._common_settings = fetch_kinds(common_namespace)
            else:
                self._common_settings = {}
        except (StopIteration, TypeError, DefaultCredentialsError):
            if force_datastore:
                raise
            # Defaults to empty dictionary. Settings will be taken from the
            # environment variables.
            self._common_settings = {}
        else:
            self._common_settings.update(self._settings)

    @staticmethod
    def check_metaconfig(kwargs, global_name, kwarg_name, required=True):
        if kwargs.get(kwarg_name):
            return kwargs[kwarg_name]
        elif os.environ.get(global_name):
            return os.environ[global_name]
        if required:
            raise KonfugMetaConfigError(global_name, kwarg_name)

    def raw_setting(self, key, default_val=None, apply_=None, nullable=False):
        if key in os.environ:
            val = os.getenv(key)
        elif key in self._common_settings:
            val = self._common_settings[key]
        elif default_val is not None:
            val = default_val
        elif not nullable:
            raise KonfugMissingError(key)
        else:
            val = None
        return apply_(val) if callable(apply_) else val

    def string(self, key, default_val=None):
        return self.raw_setting(key, default_val=default_val)

    def flag(self, key, default_val=None):
        to_bool = functools.partial(
            type(self).to_bool, falsey_expressions=self._falsey_expressions
        )
        return self.raw_setting(key, default_val=default_val, apply_=to_bool)

    def stringlist(self, key, default_val=None):
        to_stringlist = functools.partial(
            type(self).to_stringlist, sep=self._stringlist_separator
        )
        return self.raw_setting(
            key, default_val=default_val, apply_=to_stringlist
        )

    def integer(self, key, default_val=None):
        return self.raw_setting(key, default_val=default_val, apply_=int)

    def dictionary(self, key, default_val=None):
        return self.raw_setting(
            key, default_val=default_val, apply_=type(self).to_dict
        )

    def floatnum(self, key, default_val=None):
        return self.raw_setting(key, default_val=default_val, apply_=float)

    @staticmethod
    def to_bool(val, falsey_expressions=DEFAULT_FALSEY_EXPRESSIONS):
        return val not in falsey_expressions

    @staticmethod
    def to_stringlist(val, sep=','):
        return [v.strip() for v in val.split(sep) if v.strip()]

    @staticmethod
    def to_dict(val):
        try:
            # os.environ has a JSON formatted string.
            dict_ = json.loads(val)
        except TypeError:
            # Google Datastore should return a dictionary.
            dict_ = val

        if not isinstance(dict_, dict):
            raise ValueError(f'Not a dict {val}')
        else:
            return dict_

    def secret(self, key, default_val=None, encoding=UTF8):
        if key in os.environ:
            val = os.getenv(key)
        elif self._skip_secret_manager or self._secretclient is None:
            val = None
        else:
            name = self._secret_resource_name_tpl.format(secret_id=key)
            secret = self._secretclient.access_secret_version(
                                                    request={"name": name})
            val = secret.payload.data.decode(encoding)

        if val is None and default_val:
            val = default_val
        elif val is None:
            raise KonfugMissingError(key, is_secret=True)

        return val
